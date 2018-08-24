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
import com.couchbase.connector.config.es.ImmutableConnectorConfig;
import com.couchbase.connector.config.es.ImmutableElasticsearchConfig;
import com.couchbase.connector.testcontainers.CouchbaseContainer;
import fr.pilato.elasticsearch.containers.ElasticsearchContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class IntegrationTest {
  private static final String COUCHBASE_DOCKER_IMAGE = "couchbase/server:5.5.0";

  // Use dynamic ports in CI environment to avoid port conflicts
  private static final boolean DYNAMIC_PORTS = System.getenv("JENKINS_URL") != null;

  // Supply a non-zero value to use a fixed port for Couchbase web UI.
  private static final int HOST_COUCHBASE_UI_PORT = DYNAMIC_PORTS ? 0 : 8891;

  private static CouchbaseContainer couchbase;
  private static ElasticsearchContainer elasticsearch;

  @BeforeClass
  public static void setup() throws Exception {
    final Network network = Network.builder().id("dcp-test-network").build();
    couchbase = CouchbaseContainer.newCluster(COUCHBASE_DOCKER_IMAGE, network, "kv1.couchbase.host", HOST_COUCHBASE_UI_PORT);

    elasticsearch = new ElasticsearchContainer().withVersion("6.3.2");
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
  public void foo() throws Throwable {
    couchbase.loadSampleBucket("travel-sample");

    final ImmutableConnectorConfig defaultConfig = loadConfig();

    // Do I love this or hate this? Maybe a bit of both.
    final ImmutableConnectorConfig testConfig = defaultConfig
        .withElasticsearch(
            ImmutableElasticsearchConfig.copyOf(defaultConfig.elasticsearch())
                .withHosts(elasticsearch.getHost()));

    ElasticsearchConnector.run(testConfig);
    throw new RuntimeException("hahaha oops");
  }

  private ImmutableConnectorConfig loadConfig() throws IOException {
    try (InputStream is = new FileInputStream("src/dist/config/example-connector.toml")) {
      return ConnectorConfig.from(is);
    }
  }
}
