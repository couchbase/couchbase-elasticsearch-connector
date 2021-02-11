/*
 * Copyright 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connector.elasticsearch;

import com.couchbase.connector.config.es.ConnectorConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static com.couchbase.connector.elasticsearch.ElasticsearchHelper.newElasticsearchClient;
import static com.couchbase.connector.elasticsearch.io.BackoffPolicyBuilder.constantBackoff;
import static com.couchbase.connector.testcontainers.Poller.poll;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

class TestEsClient implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestEsClient.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final RestHighLevelClient client;

  public TestEsClient(ConnectorConfig config) throws Exception {
    this.client = newElasticsearchClient(config.elasticsearch(), config.trustStore());
  }

  public TestEsClient(String config) throws Exception {
    this(ConnectorConfig.from(config));
  }

  private static <T> T retryUntilSuccess(BackoffPolicy backoffPolicy, Callable<T> lambda) {
    Iterator<TimeValue> delays = backoffPolicy.iterator();
    while (true) {
      try {
        return lambda.call();
      } catch (Exception e) {
        e.printStackTrace();

        if (delays.hasNext()) {
          try {
            MILLISECONDS.sleep(delays.next().millis());
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(interrupted);
          }
        } else {
          throw new RuntimeException(new TimeoutException());
        }
      }
    }
  }

  public void deleteAllIndexes() {
    try {
      // retry for intermittent auth failure immediately after container startup :-p
      final BackoffPolicy backoffPolicy = constantBackoff(1, SECONDS).limit(10).build();

      for (String index : retryUntilSuccess(backoffPolicy, this::indexNames)) {
        if (index.startsWith(".")) {
          // ES 5.x complains if we try to delete these.
          continue;
        }
        JsonNode deletionResponse = doDelete(index);
        System.out.println("Deleted index '" + index + "' : " + deletionResponse);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public List<String> indexNames() {
    try {
      final JsonNode response = doGet("_stats");
      return ImmutableList.copyOf(response.path("indices").fieldNames());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    client.close();
  }

  public long getDocumentCount(String index) {
    try {
      JsonNode response = doGet(index + "/doc/_count");
      return response.get("count").longValue();
    } catch (ResponseException e) {
      return -1;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Optional<JsonNode> getDocument(String index, String id) {
    try {
      return Optional.of(doGet(index + "/doc/" + encodeUriPathComponent(id)));

    } catch (ResponseException e) {
      return Optional.empty();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Optional<JsonNode> getDocument(String index, String id, String routing) {
    try {
      return Optional.of(doGet(index + "/doc/" + encodeUriPathComponent(id) + "?routing=" + encodeUriQueryParameter(routing)));

    } catch (ResponseException e) {
      return Optional.empty();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public JsonNode waitForDocument(String index, String id) throws TimeoutException, InterruptedException {
    return waitForDocuments(index, singletonList(id)).get(id);
  }

  private static MultiGetRequest multiGetRequest(String index, Set<String> ids) {
    final MultiGetRequest request = new MultiGetRequest();
    for (String id : ids) {
      request.add(index, "doc", id);
    }
    return request;
  }

  public Map<String, JsonNode> waitForDocuments(String index, Collection<String> ids) throws TimeoutException, InterruptedException {
    final Stopwatch timer = Stopwatch.createStarted();

    final Map<String, JsonNode> idToDocument = new LinkedHashMap<>();
    final Set<String> remaining = new HashSet<>(ids);

    poll().atInterval(500, MILLISECONDS).until(() -> {
      try {
        final MultiGetResponse response = client.multiGet(multiGetRequest(index, remaining));
        final ListMultimap<String, String> errorMessageToDocId = ArrayListMultimap.create();
        for (MultiGetItemResponse item : response) {
          if (item.isFailed()) {
            errorMessageToDocId.put(item.getFailure().getMessage(), item.getId());
            continue;
          }
          if (item.getResponse().isExists()) {
            idToDocument.put(item.getId(), objectMapper.readTree(item.getResponse().getSourceAsBytes()));
            remaining.remove(item.getId());
          }
        }

        // Display only the first error of each kind so we don't spam the logs
        // with thousands of 'index_not_found_exception' errors
        for (String errorMessage : errorMessageToDocId.keySet()) {
          final List<String> docIds = errorMessageToDocId.get(errorMessage);
          final String firstDocId = docIds.get(0);
          final int others = docIds.size() - 1;

          if (others == 0) {
            LOGGER.warn("Failed to get document {} : {}", firstDocId, errorMessage);
          } else {
            LOGGER.warn("Failed to get document {} (and {} others) : {}", firstDocId, others, errorMessage);
          }
        }

        if (remaining.isEmpty()) {
          return true;
        }

        LOGGER.info("Still waiting for {} documents...", remaining.size());
        return false;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    LOGGER.info("Waiting for {} documents took {}", ids.size(), timer);

    return idToDocument;
  }

  public void waitForDeletion(String index, String id) throws TimeoutException, InterruptedException {
    poll().until(() -> !getDocument(index, id).isPresent());
  }

  private JsonNode doGet(String endpoint) throws IOException {
    final Response response = client.getLowLevelClient().performRequest("GET", endpoint);
    try (InputStream is = response.getEntity().getContent()) {
      return objectMapper.readTree(is);
    }
  }

  private JsonNode doDelete(String endpoint) throws IOException {
    final Response response = client.getLowLevelClient().performRequest("DELETE", endpoint);
    try (InputStream is = response.getEntity().getContent()) {
      return objectMapper.readTree(is);
    }
  }

  private static String encodeUriPathComponent(String s) {
    return encodeUriQueryParameter(s).replace("+", "%20");
  }

  private static String encodeUriQueryParameter(String s) {
    try {
      return URLEncoder.encode(s, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError("UTF-8 not supported???", e);
    }
  }
}
