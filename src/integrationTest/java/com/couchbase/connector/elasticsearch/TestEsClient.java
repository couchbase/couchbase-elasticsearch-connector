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
import com.couchbase.connector.elasticsearch.sink.SinkOps;
import com.couchbase.connector.elasticsearch.sink.SinkTestOps;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static com.couchbase.connector.elasticsearch.ElasticsearchHelper.newElasticsearchClient;
import static com.couchbase.connector.testcontainers.Poller.poll;
import static java.util.Collections.singletonList;

class TestEsClient implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestEsClient.class);

  private final SinkTestOps sinkOps;
  private final ElasticsearchSinkOps elasticsearchSinkOps;

  public TestEsClient(ConnectorConfig config) {
    this.sinkOps = (SinkTestOps) SinkOps.create(config);

    // For some operations, it's easier to use the Elasticsearch client,
    // because it can easily make ad-hoc HTTP calls.
    this.elasticsearchSinkOps = newElasticsearchClient(config);
  }

  public TestEsClient(String config) {
    this(ConnectorConfig.from(config));
  }

  public void deleteAllIndexes() {
    elasticsearchSinkOps.deleteAllIndexes();
  }

  @Override
  public void close() throws Exception {
    sinkOps.close();
    elasticsearchSinkOps.close();
  }

  public long getDocumentCount(String index) {
    return sinkOps.countDocuments(index);
  }

  public Optional<JsonNode> getDocument(String index, String id) {
    return sinkOps.getDocument(index, id, null);
  }

  public Optional<JsonNode> getDocument(String index, String id, @Nullable String routing) {
    return sinkOps.getDocument(index, id, routing);
  }

  public JsonNode waitForDocument(String index, String id) throws TimeoutException, InterruptedException {
    return waitForDocuments(index, singletonList(id)).get(id);
  }

  public Map<String, JsonNode> waitForDocuments(String index, Collection<String> ids) throws TimeoutException, InterruptedException {
    final Stopwatch timer = Stopwatch.createStarted();

    final Map<String, JsonNode> idToDocument = new LinkedHashMap<>();
    final List<String> remaining = new ArrayList<>(new HashSet<>(ids));

    poll().atInterval(Duration.ofMillis(500)).until(() -> {
      List<SinkTestOps.MultiGetItem> response = sinkOps.multiGet(index, ids);

      final ListMultimap<String, String> errorMessageToDocId = ArrayListMultimap.create();
      for (SinkTestOps.MultiGetItem item : response) {
        if (item.error != null) {
          errorMessageToDocId.put(item.error, item.id);
          continue;
        }

        if (item.document != null) {
          idToDocument.put(item.id, item.document);
          remaining.remove(item.id);
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
    });

    LOGGER.info("Waiting for {} documents took {}", ids.size(), timer);

    return idToDocument;
  }

  public void waitForDeletion(String index, String id) throws TimeoutException, InterruptedException {
    poll().until(() -> getDocument(index, id).isEmpty());
  }

}
