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
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static com.couchbase.connector.elasticsearch.ElasticsearchHelper.newElasticsearchClient;
import static com.couchbase.connector.testcontainers.Poller.poll;

class TestEsClient implements AutoCloseable {
  private final RestHighLevelClient client;

  public TestEsClient(ConnectorConfig config) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
    this.client = newElasticsearchClient(config.elasticsearch(), config.trustStore());
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

  private static String encodeUriPathComponent(String s) {
    try {
      return URLEncoder.encode(s, StandardCharsets.UTF_8.name()).replace("+", "%20");
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError("UTF-8 not supported???", e);
    }
  }
}
