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
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.connector.config.common.ImmutableCouchbaseConfig;
import com.couchbase.connector.config.common.ImmutableMetricsConfig;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.config.es.ImmutableConnectorConfig;
import com.couchbase.connector.config.es.ImmutableElasticsearchConfig;
import com.couchbase.connector.dcp.CouchbaseHelper;
import com.couchbase.connector.elasticsearch.cli.CheckpointClear;
import com.couchbase.connector.testcontainers.CustomCouchbaseContainer;
import com.couchbase.connector.testcontainers.ElasticsearchContainer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.CharStreams;
import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.couchbase.connector.dcp.CouchbaseHelper.environmentBuilder;
import static com.couchbase.connector.dcp.CouchbaseHelper.forceKeyToPartition;
import static com.couchbase.connector.testcontainers.CustomCouchbaseContainer.newCouchbaseCluster;
import static com.couchbase.connector.testcontainers.Poller.poll;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * MUST NOT be run in parallel.
 */
@RunWith(Parameterized.class)
public class BasicReplicationTest {

//  static {
//    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
//  }

  private static String cachedCouchbaseContainerVersion;
  private static String cachedElasticsearchContainerVersion;

  private static CustomCouchbaseContainer couchbase;
  private static ElasticsearchContainer elasticsearch;
  private static ImmutableConnectorConfig commonConfig;

  @Parameterized.Parameters(name = "cb={0}, es={1}")
  public static Iterable<Object[]> versionsToTest() {
    // Only the first version in each list will be tested, unless this condition is `true`
    final boolean exhaustive = Boolean.valueOf(System.getProperty("com.couchbase.integrationTest.exhaustive"));

    final ImmutableSet<String> couchbaseVersions = ImmutableSet.of(
//        "enterprise-6.0.1",
//        "enterprise-5.5.1",
//        "enterprise-5.5.0",
//        "enterprise-5.1.1",
//        "community-6.0.0",
//        "community-5.1.1",
//        "enterprise-5.1.0",
//        "enterprise-5.0.1",
        "community-5.0.1");

    final ImmutableSet<String> elasticsearchVersions = ImmutableSet.of(
//        "7.0.0-beta1",
//        "6.6.0",
//        "6.5.4",
//        "6.4.3",
//        "6.3.2",
//        "6.2.4",
//        "6.1.4",
//        "6.0.1",
//        "5.6.11",
//        "5.5.3",
//        "5.4.3",
//        "5.3.3",
          "5.3.3"
//        "5.2.2"
    );

    if (!exhaustive) {
      // just test the most recent versions
      return ImmutableList.of(new Object[]{
          Iterables.get(couchbaseVersions, 0),
          Iterables.get(elasticsearchVersions, 0)});
    }

    // Full cartesian product is overkill; just test every supported version
    // at least once in some combination.
    final List<Object[]> combinations = new ArrayList<>();

    final String newestCb = Iterables.get(couchbaseVersions, 0);
    final Iterable<String> olderCouchbaseVersions = Iterables.skip(couchbaseVersions, 1);
    final String newestEs = Iterables.get(elasticsearchVersions, 0);

    // Prefer repeating Couchbase versions, since the CB container is more expensive to set up than ES.
    elasticsearchVersions.forEach(es -> combinations.add(new Object[]{newestCb, es}));
    olderCouchbaseVersions.forEach(cb -> combinations.add(new Object[]{cb, newestEs}));

    return combinations;
  }

  private final String couchbaseVersion;
  private final String elasticsearchVersion;

  public BasicReplicationTest(String couchbaseVersion, String elasticsearchVersion) {
    this.couchbaseVersion = requireNonNull(couchbaseVersion);
    this.elasticsearchVersion = requireNonNull(elasticsearchVersion);
  }

  @Before
  public void setup() throws Exception {
    if (couchbaseVersion.equals(cachedCouchbaseContainerVersion)) {
      System.out.println("Using cached Couchbase container: " + couchbase.getDockerImageName() +
          " listening at http://localhost:" + couchbase.getMappedPort(8091));
    } else {
      stop(couchbase);
      couchbase = newCouchbaseCluster("couchbase/server:" + couchbaseVersion);

      System.out.println("Couchbase " + couchbase.getVersionString() +
          " listening at http://localhost:" + couchbase.getMappedPort(8091));

      cachedCouchbaseContainerVersion = couchbaseVersion;
      couchbase.loadSampleBucket("travel-sample", 100);
    }

    if (elasticsearchVersion.equals(cachedElasticsearchContainerVersion)) {
      System.out.println("Using cached Elasticsearch container " + elasticsearch.getDockerImageName() +
          " listening at " + elasticsearch.getHost());
    } else {
      stop(elasticsearch);
      elasticsearch = new ElasticsearchContainer(Version.fromString(elasticsearchVersion))
          .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("container.elasticsearch")));
      elasticsearch.start();
      System.out.println("Elasticsearch listening at " + elasticsearch.getHost());
      cachedElasticsearchContainerVersion = elasticsearchVersion;
    }

    commonConfig = patchConfigForTesting(loadConfig(), couchbase, elasticsearch);
    CheckpointClear.clear(commonConfig);

    try (TestEsClient es = new TestEsClient(commonConfig)) {
      es.deleteAllIndexes();
    }
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
                                                                CustomCouchbaseContainer couchbase,
                                                                ElasticsearchContainer elasticsearch) {
    final String dockerHost = getDockerHost();

    return config
        .withMetrics(ImmutableMetricsConfig.builder()
            .httpPort(-1)
            .logInterval(TimeValue.ZERO)
            .build())
        .withElasticsearch(
            ImmutableElasticsearchConfig.copyOf(config.elasticsearch())
                .withHosts(new HttpHost(dockerHost, elasticsearch.getHost().getPort())))
        .withCouchbase(
            ImmutableCouchbaseConfig.copyOf(config.couchbase())
                .withHosts(dockerHost + ":" + couchbase.getMappedPort(8091))
        );
  }

  private static String getDockerHost() {
    final String env = System.getenv("DOCKER_HOST");
    if (Strings.isNullOrEmpty(env)) {
      return "localhost";
    }

    try {
      return env.contains("://") ? new URI(env).getHost() : env;
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private static ImmutableConnectorConfig withBucketName(ImmutableConnectorConfig config, String bucketName) {
    return config.withCouchbase(
        ImmutableCouchbaseConfig.copyOf(config.couchbase())
            .withBucket(bucketName));
  }

  private static CouchbaseCluster createTestCluster(ImmutableConnectorConfig config) {
    // xxx orphans the environment.
    final CouchbaseEnvironment env = environmentBuilder(config.couchbase(), config.trustStore())
        .mutationTokensEnabled(true)
        .build();
    return CouchbaseHelper.createCluster(config.couchbase(), env);
  }

  @Test
  public void createDeleteReject() throws Throwable {
    final CouchbaseCluster cluster = createTestCluster(commonConfig);

    try (TempBucket tempBucket = new TempBucket(couchbase)) {
      ImmutableConnectorConfig config = withBucketName(commonConfig, tempBucket.name());

      try (TestEsClient es = new TestEsClient(config);
           TestConnector connector = new TestConnector(config).start()) {

        final Bucket bucket = cluster.openBucket(tempBucket.name());

        assertIndexInferredFromDocumentId(bucket, es);

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

  /**
   * Verify the type definition that infers index from ID is working as expected.
   */
  private void assertIndexInferredFromDocumentId(Bucket bucket, TestEsClient es) throws Exception {
    bucket.upsert(JsonDocument.create("widget::123", JsonObject.create()));
    bucket.upsert(JsonDocument.create("widget::foo::bar", JsonObject.create()));
    es.waitForDocument("widget", "widget::123");
    es.waitForDocument("widget", "widget::foo::bar");
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
