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
import com.couchbase.connector.elasticsearch.OpenSearchSinkOps;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.core5.io.CloseMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.connector.elasticsearch.ElasticsearchHelper.newElasticsearchSinkOps;
import static com.couchbase.connector.elasticsearch.OpenSearchHelper.newAwsOpenSearchSinkOps;
import static com.couchbase.connector.elasticsearch.OpenSearchHelper.newHttpClient;
import static com.couchbase.connector.elasticsearch.OpenSearchHelper.newOpenSearchSinkOps;

public interface SinkOps extends Closeable {
  Logger log = LoggerFactory.getLogger(SinkOps.class);

  Version version();

  default void ping() {
    version();
  }

  SinkBulkResponse bulk(List<Operation> operations) throws IOException;

  static SinkOps create(ConnectorConfig config) {
    boolean looksLikeAws = config.elasticsearch().hosts().get(0).getHostName().endsWith(".es.amazonaws.com");
    if (looksLikeAws && config.elasticsearch().aws().region().isEmpty()) {
      log.warn(
          "It looks like you might be trying to connect to an Amazon OpenSearch Service domain." +
              " If this is the case, please specify the domain's AWS region" +
              " in the connector's [elasticsearch.aws] config section." +
              " If you are not connecting to an Amazon OpenSearch Service domain, please disregard this warning."
      );
    }

    // If an AWS region is specified, we know to use an OpenSearch client.
    // This client works with both OpenSearch and old versions of Elasticsearch.
    if (!config.elasticsearch().aws().region().isEmpty()) {
      log.info("Connector config specifies an AWS region; activating Amazon OpenSearch Service mode.");
      OpenSearchSinkOps sinkOps = newAwsOpenSearchSinkOps(config);

      log.info("Verifying connectivity to Amazon OpenSearch Service domain...");
      ObjectNode info = sinkOps.info();
      log.info("Successfully connected to Amazon OpenSearch Service. Sink info: {}", redactSystem(info));
      return sinkOps;
    }

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
        sinkOps = newOpenSearchSinkOps(httpClient, config);
        break;
      case ELASTICSEARCH:
        sinkOps = newElasticsearchSinkOps(config);
        break;
      default:
        throw new RuntimeException("Unrecognized sink: " + fav);
    }

    // Verify client connectivity
    sinkOps.ping();

    return sinkOps;
  }
}
