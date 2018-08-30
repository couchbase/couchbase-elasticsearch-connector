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

import com.couchbase.connector.config.common.ImmutableCouchbaseConfig;
import com.couchbase.connector.config.common.ImmutableMetricsConfig;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.config.es.ImmutableConnectorConfig;
import com.couchbase.connector.config.es.ImmutableElasticsearchConfig;
import com.couchbase.connector.testcontainers.CouchbaseContainer;
import com.couchbase.connector.testcontainers.ElasticsearchContainer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CharStreams;
import org.elasticsearch.Version;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.connector.elasticsearch.ElasticsearchHelper.newElasticsearchClient;
import static com.couchbase.connector.testcontainers.Poller.poll;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class BasicReplicationTest {
  private static final String COUCHBASE_DOCKER_IMAGE = "couchbase/server:5.5.0";
  private static final Version ELASTICSEARCH_VERSION = Version.fromString("6.4.0");

  private static CouchbaseContainer couchbase;
  private static ElasticsearchContainer elasticsearch;

  @BeforeClass
  public static void setup() throws Exception {
    couchbase = CouchbaseContainer.newCluster(COUCHBASE_DOCKER_IMAGE);
    System.out.println("Couchbase listening at http://localhost:" + couchbase.managementPort());

    elasticsearch = new ElasticsearchContainer(ELASTICSEARCH_VERSION);
    elasticsearch.start();
    System.out.println("Elasticsearch listening on " + elasticsearch.getHost());
  }

  @AfterClass
  public static void cleanup() throws Exception {
    stop(couchbase, elasticsearch);
  }

  protected static void stop(GenericContainer first, GenericContainer... others) {
    if (first != null) {
      first.stop();
    }
    for (GenericContainer c : others) {
      if (c != null) {
        c.stop();
      }
    }
  }

  @Test
  public void canReplicateTravelSample() throws Throwable {
    couchbase.loadSampleBucket("travel-sample");

    final ImmutableConnectorConfig defaultConfig = loadConfig();

    // Do I love this or hate this? Maybe a bit of both.
    final ImmutableConnectorConfig testConfig = defaultConfig
        .withMetrics(ImmutableMetricsConfig.builder()
            .httpPort(-1)
            .logInterval(TimeValue.ZERO)
            .build())
        .withElasticsearch(
            ImmutableElasticsearchConfig.copyOf(defaultConfig.elasticsearch())
                .withHosts(elasticsearch.getHost()))
        .withCouchbase(
            ImmutableCouchbaseConfig.copyOf(defaultConfig.couchbase())
                .withHosts("localhost:" + couchbase.managementPort())
        )
        //
        ;

    final AtomicReference<Throwable> connectorException = new AtomicReference<>();

    final Thread connectorThread = new Thread(() -> {
      try {
        ElasticsearchConnector.run(testConfig);
      } catch (Throwable t) {
        if (!(t instanceof InterruptedException)) {
          t.printStackTrace();
        }
        connectorException.set(t);
      }
    });

    connectorThread.start();

    try (RestClient restClient = newElasticsearchClient(testConfig.elasticsearch(), testConfig.trustStore())
        .getLowLevelClient()) {

      final int expectedAirlineCount = 187;
      final int expectedAirportCount = 1968;

      poll().until(() -> getDocumentCount(restClient, "airlines") >= expectedAirlineCount);
      poll().until(() -> getDocumentCount(restClient, "airports") >= expectedAirportCount);

      SECONDS.sleep(3); // quiet period, make sure no more documents appear in the index

      assertEquals(expectedAirlineCount, getDocumentCount(restClient, "airlines"));
      assertEquals(expectedAirportCount, getDocumentCount(restClient, "airports"));

      connectorThread.interrupt();
      connectorThread.join(SECONDS.toMillis(30));
      assertEquals(connectorException.get().getClass(), InterruptedException.class);

    } catch (Throwable t) {
      connectorThread.interrupt();
      throw t;
    }
  }

  private ImmutableConnectorConfig loadConfig() throws IOException {
    try (InputStream is = new FileInputStream("src/dist/config/example-connector.toml")) {
      // rewrite the document type config for compatibility with ES 5.x
      StringBuilder sb = new StringBuilder();
      CharStreams.copy(new InputStreamReader(is, UTF_8), sb);
      String config = sb.toString().replace("'_doc'", "'doc'");
      return ConnectorConfig.from(new ByteArrayInputStream(config.getBytes(UTF_8)));
    }
  }

  private static long getDocumentCount(RestClient client, String index) {
    try {
      JsonNode response = doGet(client, index + "/doc/_count");
      return response.get("count").longValue();
    } catch (ResponseException e) {
      return -1;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static JsonNode doGet(RestClient client, String endpoint) throws IOException {
    final Response response = client.performRequest("GET", endpoint);
    try (InputStream is = response.getEntity().getContent()) {
      return new ObjectMapper().readTree(is);
    }
  }
}
