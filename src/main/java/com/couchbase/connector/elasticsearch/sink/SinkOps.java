/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connector.elasticsearch.sink;

import com.couchbase.client.dcp.util.Version;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.elasticsearch.ElasticsearchVersionSniffer;
import com.couchbase.connector.elasticsearch.OpenSearchHelper;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.core5.io.CloseMode;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static com.couchbase.connector.elasticsearch.ElasticsearchHelper.newElasticsearchClient;
import static com.couchbase.connector.elasticsearch.OpenSearchHelper.newHttpClient;
import static com.couchbase.connector.elasticsearch.OpenSearchHelper.newOpenSearchClient;

public interface SinkOps extends Closeable {
  Version version();

  default void ping() {
    version();
  }

  SinkBulkResponse bulk(List<Operation> operations) throws IOException;

  static SinkOps create(ConnectorConfig config) {
    final CloseableHttpAsyncClient httpClient = newHttpClient(config);

    ElasticsearchVersionSniffer.FlavorAndVersion fav = new ElasticsearchVersionSniffer(httpClient).sniff(
        OpenSearchHelper.hosts(config.elasticsearch()),
        Duration.ofMinutes(2)
    );

    if (fav.flavor != ElasticsearchVersionSniffer.Flavor.OPENSEARCH) {
      // Don't need this HTTP client anymore!
      httpClient.close(CloseMode.IMMEDIATE);
    }

    final SinkOps sinkOps;
    switch (fav.flavor) {
      case OPENSEARCH:
        sinkOps = newOpenSearchClient(httpClient, config);
        break;
      case ELASTICSEARCH:
        sinkOps = newElasticsearchClient(config);
        break;
      default:
        throw new RuntimeException("Unrecognized sink: " + fav);
    }

    // Verify client connectivity
    sinkOps.ping();

    return sinkOps;
  }
}
