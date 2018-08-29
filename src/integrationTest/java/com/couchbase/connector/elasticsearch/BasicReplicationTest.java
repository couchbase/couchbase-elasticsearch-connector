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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.pilato.elasticsearch.containers.ElasticsearchContainer;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.connector.elasticsearch.ElasticsearchHelper.newElasticsearchClient;
import static com.couchbase.connector.testcontainers.Poller.poll;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class BasicReplicationTest {
  private static final String COUCHBASE_DOCKER_IMAGE = "couchbase/server:5.5.0";

  private static CouchbaseContainer couchbase;
  private static ElasticsearchContainer elasticsearch;

  @BeforeClass
  public static void setup() throws Exception {
    couchbase = CouchbaseContainer.newCluster(COUCHBASE_DOCKER_IMAGE);

    elasticsearch = new ElasticsearchContainer().withVersion("6.4.0");
    elasticsearch.start();
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
      return ConnectorConfig.from(is);
    }
  }

  private static long getDocumentCount(RestClient client, String index) {
    try {
      JsonNode response = doGet(client, index + "/_doc/_count");
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
