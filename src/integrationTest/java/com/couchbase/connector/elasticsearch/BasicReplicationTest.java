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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.connector.config.common.ImmutableCouchbaseConfig;
import com.couchbase.connector.config.common.ImmutableMetricsConfig;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.config.es.ImmutableConnectorConfig;
import com.couchbase.connector.config.es.ImmutableElasticsearchConfig;
import com.couchbase.connector.dcp.CouchbaseHelper;
import com.couchbase.connector.testcontainers.CouchbaseContainer;
import com.couchbase.connector.testcontainers.ElasticsearchContainer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.CharStreams;
import org.elasticsearch.Version;
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
import java.util.concurrent.TimeoutException;

import static com.couchbase.connector.dcp.CouchbaseHelper.forceKeyToPartition;
import static com.couchbase.connector.testcontainers.Poller.poll;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BasicReplicationTest {
  private static final String COUCHBASE_DOCKER_IMAGE = "couchbase/server:community-5.0.1";
  private static final Version ELASTICSEARCH_VERSION = Version.fromString("6.4.0");

  private static CouchbaseContainer couchbase;
  private static ElasticsearchContainer elasticsearch;
  private static ImmutableConnectorConfig commonConfig;

  @BeforeClass
  public static void setup() throws Exception {
    couchbase = CouchbaseContainer.newCluster(COUCHBASE_DOCKER_IMAGE);
    System.out.println("Couchbase listening at http://localhost:" + couchbase.managementPort());

    elasticsearch = new ElasticsearchContainer(ELASTICSEARCH_VERSION);
    elasticsearch.start();
    System.out.println("Elasticsearch listening on " + elasticsearch.getHost());

    commonConfig = patchConfigForTesting(loadConfig(), couchbase, elasticsearch);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    stop(couchbase, elasticsearch);
  }

  private static void stop(GenericContainer first, GenericContainer... others) {
    if (first != null) {
      first.stop();
    }
    for (GenericContainer c : others) {
      if (c != null) {
        c.stop();
      }
    }
  }

  private static ImmutableConnectorConfig patchConfigForTesting(ImmutableConnectorConfig config,
                                                                CouchbaseContainer couchbase,
                                                                ElasticsearchContainer elasticsearch) {
    return config
        .withMetrics(ImmutableMetricsConfig.builder()
            .httpPort(-1)
            .logInterval(TimeValue.ZERO)
            .build())
        .withElasticsearch(
            ImmutableElasticsearchConfig.copyOf(config.elasticsearch())
                .withHosts(elasticsearch.getHost()))
        .withCouchbase(
            ImmutableCouchbaseConfig.copyOf(config.couchbase())
                .withHosts("localhost:" + couchbase.managementPort())
        );
  }

  private static ImmutableConnectorConfig withBucketName(ImmutableConnectorConfig config, String bucketName) {
    return config.withCouchbase(
        ImmutableCouchbaseConfig.copyOf(config.couchbase())
            .withBucket(bucketName));
  }

  private static CouchbaseCluster createTestCluster(ImmutableConnectorConfig config) {
    return CouchbaseHelper.createCluster(config.couchbase(), config.trustStore(),
        env -> env.mutationTokensEnabled(true));
  }

  @Test
  public void createDeleteReject() throws Throwable {
    final CouchbaseCluster cluster = createTestCluster(commonConfig);

    try (TempBucket tempBucket = new TempBucket(couchbase)) {
      ImmutableConnectorConfig config = withBucketName(commonConfig, tempBucket.name());

      try (TestEsClient es = new TestEsClient(config);
           TestConnector connector = new TestConnector(config).start()) {

        final Bucket bucket = cluster.openBucket(tempBucket.name());

        // Create two documents in the same vbucket to make sure we're not conflating seqno and revision number.
        // This first one has a seqno and revision number that are the same... not useful for the test.
        final String firstKeyInVbucket = forceKeyToPartition("createdFirst", 0, 1024).get();
        bucket.upsert(JsonDocument.create(firstKeyInVbucket, JsonObject.create()));

        // Here's the document we're going to test! Its seqno should be different than document revision.
        final String blueKey = forceKeyToPartition("color:blue", 0, 1024).get();
        final JsonDocument doc = bucket.upsert(JsonDocument.create(blueKey, JsonObject.create().put("hex", "0000ff")));
        final JsonNode content = es.waitForDocument("etc", blueKey);

        System.out.println(content);

        final long expectedDocumentRevision = 1;

        final JsonNode meta = content.path("meta");
        assertEquals(blueKey, meta.path("id").textValue());
        assertEquals(expectedDocumentRevision, meta.path("revSeqno").longValue());
        assertTrue(meta.path("lockTime").isIntegralNumber());
        assertEquals(0, meta.path("lockTime").longValue());
        assertTrue(meta.path("expiration").isIntegralNumber());
        assertEquals(0, meta.path("expiration").longValue());
        assertThat(meta.path("rev").textValue()).startsWith(expectedDocumentRevision + "-");
        assertTrue(meta.path("flags").isIntegralNumber());
        assertEquals(doc.cas(), meta.path("cas").longValue());
        assertEquals(doc.mutationToken().sequenceNumber(), meta.path("seqno").longValue());
        assertEquals(doc.mutationToken().vbucketID(), meta.path("vbucket").longValue());
        assertEquals(doc.mutationToken().vbucketUUID(), meta.path("vbuuid").longValue());

        assertEquals("0000ff", content.path("doc").path("hex").textValue());

        // Make sure deletions are propagated to elasticsearch
        bucket.remove(blueKey);
        es.waitForDeletion("etc", blueKey);

        // Create an incompatible document (different type for "hex" field, Object instead of String)
        final String redKey = "color:red";
        bucket.upsert(JsonDocument.create(redKey, JsonObject.create()
            .put("hex", JsonObject.create()
                .put("red", "ff")
                .put("green", "00")
                .put("blue", "00")
            )));
        assertDocumentRejected(es, "etc", redKey, "mapper_parsing_exception");

      } finally {
        cluster.disconnect();
      }
    }
  }

  private static void assertDocumentRejected(TestEsClient es, String index, String id, String reason) throws TimeoutException, InterruptedException {
    assertFalse(es.getDocument(index, id).isPresent());

    final JsonNode content = es.waitForDocument("cbes-rejects", id);
    System.out.println(content);

    assertEquals(index, content.path("index").textValue());
    assertEquals("doc", content.path("type").textValue());
    assertEquals("INDEX", content.path("action").textValue());
    assertThat(content.path("error").textValue()).contains(reason);
  }

  @Test
  public void canReplicateTravelSample() throws Throwable {
    couchbase.loadSampleBucket("travel-sample");

    try (TestEsClient es = new TestEsClient(commonConfig);
         TestConnector connector = new TestConnector(commonConfig).start()) {

      final int expectedAirlineCount = 187;
      final int expectedAirportCount = 1968;

      poll().until(() -> es.getDocumentCount("airlines") >= expectedAirlineCount);
      poll().until(() -> es.getDocumentCount("airports") >= expectedAirportCount);

      SECONDS.sleep(3); // quiet period, make sure no more documents appear in the index

      assertEquals(expectedAirlineCount, es.getDocumentCount("airlines"));
      assertEquals(expectedAirportCount, es.getDocumentCount("airports"));
    }
  }

  private static ImmutableConnectorConfig loadConfig() throws IOException {
    try (InputStream is = new FileInputStream("src/dist/config/example-connector.toml")) {
      // rewrite the document type config for compatibility with ES 5.x
      StringBuilder sb = new StringBuilder();
      CharStreams.copy(new InputStreamReader(is, UTF_8), sb);
      String config = sb.toString().replace("'_doc'", "'doc'");
      return ConnectorConfig.from(new ByteArrayInputStream(config.getBytes(UTF_8)));
    }
  }
}
