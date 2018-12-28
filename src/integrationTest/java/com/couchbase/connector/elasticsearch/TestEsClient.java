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
import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static com.couchbase.connector.elasticsearch.ElasticsearchHelper.newElasticsearchClient;
import static com.couchbase.connector.elasticsearch.io.BackoffPolicyBuilder.constantBackoff;
import static com.couchbase.connector.testcontainers.Poller.poll;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

class TestEsClient implements AutoCloseable {
  private final RestHighLevelClient client;

  public TestEsClient(ConnectorConfig config) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
    this.client = newElasticsearchClient(config.elasticsearch(), config.trustStore());
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

  public JsonNode waitForDocument(String index, String id) throws TimeoutException, InterruptedException {
    poll().until(() -> getDocument(index, id).isPresent());
    return getDocument(index, id).get().get("_source");
  }

  public void waitForDeletion(String index, String id) throws TimeoutException, InterruptedException {
    poll().until(() -> !getDocument(index, id).isPresent());
  }

  private JsonNode doGet(String endpoint) throws IOException {
    final Response response = client.getLowLevelClient().performRequest("GET", endpoint);
    try (InputStream is = response.getEntity().getContent()) {
      return new ObjectMapper().readTree(is);
    }
  }

  private JsonNode doDelete(String endpoint) throws IOException {
    final Response response = client.getLowLevelClient().performRequest("DELETE", endpoint);
    try (InputStream is = response.getEntity().getContent()) {
      return new ObjectMapper().readTree(is);
    }
  }

  private static String encodeUriPathComponent(String s) {
    try {
      return URLEncoder.encode(s, StandardCharsets.UTF_8.name()).replace("+", "%20");
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError("UTF-8 not supported???", e);
    }
  }
}
