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

import co.elastic.clients.elasticsearch.core.MgetResponse;
import co.elastic.clients.elasticsearch.core.get.GetResult;
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.elasticsearch.io.BackoffPolicy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static com.couchbase.connector.elasticsearch.ElasticsearchHelper.newElasticsearchClient;
import static com.couchbase.connector.elasticsearch.io.BackoffPolicyBuilder.constantBackoff;
import static com.couchbase.connector.testcontainers.Poller.poll;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class TestEsClient implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestEsClient.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final ElasticsearchOps client;

  public TestEsClient(ConnectorConfig config) throws Exception {
    this.client = newElasticsearchClient(config);
  }

  public TestEsClient(String config) throws Exception {
    this(ConnectorConfig.from(config));
  }

  static <T> T retryUntilSuccess(BackoffPolicy backoffPolicy, Callable<T> lambda) {
    Iterator<Duration> delays = backoffPolicy.iterator();
    while (true) {
      try {
        return lambda.call();
      } catch (Exception e) {
        e.printStackTrace();

        if (delays.hasNext()) {
          try {
            MILLISECONDS.sleep(delays.next().toMillis());
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
      final BackoffPolicy backoffPolicy = constantBackoff(Duration.ofSeconds(1)).limit(10).build();

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
      JsonNode response = doGet(index + "/_count");
      return response.get("count").longValue();
    } catch (ResponseException e) {
      return -1;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Optional<JsonNode> getDocument(String index, String id) {
    try {
      return Optional.of(doGet(index + "/_doc/" + encodeUriPathComponent(id)));

    } catch (ResponseException e) {
      return Optional.empty();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Optional<JsonNode> getDocument(String index, String id, String routing) {
    try {
      return Optional.of(doGet(index + "/_doc/" + encodeUriPathComponent(id) + "?routing=" + encodeUriQueryParameter(routing)));

    } catch (ResponseException e) {
      return Optional.empty();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public JsonNode waitForDocument(String index, String id) throws TimeoutException, InterruptedException {
    return waitForDocuments(index, singletonList(id)).get(id);
  }

  public Map<String, JsonNode> waitForDocuments(String index, Collection<String> ids) throws TimeoutException, InterruptedException {
    final Stopwatch timer = Stopwatch.createStarted();

    final Map<String, JsonNode> idToDocument = new LinkedHashMap<>();
    final List<String> remaining = new ArrayList<>(new HashSet<>(ids));

    poll().atInterval(Duration.ofMillis(500)).until(() -> {
      try {
        final MgetResponse<JsonNode> response = client.modernClient().mget(
            builder -> builder
                .index(index)
                .ids(remaining),
            JsonNode.class
        );

        final ListMultimap<String, String> errorMessageToDocId = ArrayListMultimap.create();
        for (MultiGetResponseItem<JsonNode> item : response.docs()) {
          if (item.isFailure()) {
            errorMessageToDocId.put(item.failure().error().reason(), item.failure().id());
            continue;
          }

          GetResult<JsonNode> result = item.result();
          if (result.found()) {
            idToDocument.put(result.id(), item.result().source());
            remaining.remove(result.id());
          }
        }

        // Display only the first error of each kind, so we don't spam the logs
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
    poll().until(() -> getDocument(index, id).isEmpty());
  }

  private JsonNode doGet(String endpoint) throws IOException {
    final Response response = client.lowLevelClient().performRequest(new Request("GET", endpoint));
    try (InputStream is = response.getEntity().getContent()) {
      return objectMapper.readTree(is);
    }
  }

  private JsonNode doDelete(String endpoint) throws IOException {
    final Response response = client.lowLevelClient().performRequest(new Request("DELETE", endpoint));
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
