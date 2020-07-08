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

import com.couchbase.client.deps.io.netty.util.ResourceLeakDetector;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.connector.cluster.consul.AsyncTask;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.config.es.ImmutableConnectorConfig;
import com.couchbase.connector.elasticsearch.cli.CheckpointClear;
import com.couchbase.connector.testcontainers.CustomCouchbaseContainer;
import com.couchbase.connector.testcontainers.ElasticsearchContainer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.elasticsearch.Version;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static com.couchbase.connector.dcp.CouchbaseHelper.forceKeyToPartition;
import static com.couchbase.connector.elasticsearch.IntegrationTestHelper.close;
import static com.couchbase.connector.elasticsearch.IntegrationTestHelper.upsertWithRetry;
import static com.couchbase.connector.elasticsearch.IntegrationTestHelper.waitForTravelSampleReplication;
import static com.couchbase.connector.elasticsearch.TestConfigHelper.readConfig;
import static com.couchbase.connector.elasticsearch.TestConfigHelper.withBucketName;
import static com.couchbase.connector.testcontainers.CustomCouchbaseContainer.newCouchbaseCluster;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * MUST NOT be run in parallel.
 */
@RunWith(Parameterized.class)
public class BasicReplicationTest {

  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  static String CATCH_ALL_INDEX = "@default._default";

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
        "enterprise-6.5.1",
        "community-6.5.1",
        "enterprise-6.5.0",
        "community-6.5.0");

    // Need to figure out how to run the following in Docker with Testcontainers 1.14.1.
    // They don't support alternate addresses, so we can't use CouchbaseContainer anymore.
//
//        "enterprise-6.0.1",
//        "enterprise-5.5.1",
//        "enterprise-5.5.0",
//        "enterprise-5.1.1",
//        "community-6.0.0",
//        "community-5.1.1",
//        "enterprise-5.1.0",
//        "enterprise-5.0.1",
//        "community-5.0.1"


    // This list is informed by https://www.elastic.co/support/eol
    // If possible, we also want to support the last minor of every major (like 5.6.16).
    final Set<String> elasticsearchVersions = new LinkedHashSet<>(Arrays.asList(
        "7.7.0",
        "7.6.2",
        "7.5.2",
        "7.4.2",
        "7.3.2",
        "7.2.1",
        "7.1.1",
        "7.0.1",
        "6.8.6",
        "6.7.2",
        "6.6.2",
        "5.6.16"
    ));

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
      close(couchbase);
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
      close(elasticsearch);
      elasticsearch = new ElasticsearchContainer(Version.fromString(elasticsearchVersion))
          .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("container.elasticsearch")));
      elasticsearch.start();
      System.out.println("Elasticsearch listening at " + elasticsearch.getHost());
      cachedElasticsearchContainerVersion = elasticsearchVersion;
    }

    commonConfig = ConnectorConfig.from(readConfig(couchbase, elasticsearch));
    CheckpointClear.clear(commonConfig);

    try (TestEsClient es = new TestEsClient(commonConfig)) {
      es.deleteAllIndexes();
    }
  }

  @AfterClass
  public static void cleanup() throws Exception {
    close(couchbase, elasticsearch);
  }

  @Test
  public void createDeleteReject() throws Throwable {
    try (TestCouchbaseClient cb = new TestCouchbaseClient(commonConfig)) {
      final Bucket bucket = cb.createTempBucket(couchbase);
      final ImmutableConnectorConfig config = withBucketName(commonConfig, bucket.name());

      try (TestEsClient es = new TestEsClient(config);
           AsyncTask connector = AsyncTask.run(() -> ElasticsearchConnector.run(config))) {

        assertIndexInferredFromDocumentId(bucket, es);

        // Create two documents in the same vbucket to make sure we're not conflating seqno and revision number.
        // This first one has a seqno and revision number that are the same... not useful for the test.
        final String firstKeyInVbucket = forceKeyToPartition("createdFirst", 0, 1024).get();
        upsertWithRetry(bucket, JsonDocument.create(firstKeyInVbucket, JsonObject.create()));

        // Here's the document we're going to test! Its seqno should be different than document revision.
        final String blueKey = forceKeyToPartition("color:blue", 0, 1024).get();
        final JsonDocument doc = upsertWithRetry(bucket, JsonDocument.create(blueKey, JsonObject.create().put("hex", "0000ff")));
        final JsonNode content = es.waitForDocument(CATCH_ALL_INDEX, blueKey);

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
        es.waitForDeletion(CATCH_ALL_INDEX, blueKey);

        // Create an incompatible document (different type for "hex" field, Object instead of String)
        final String redKey = "color:red";
        upsertWithRetry(bucket, JsonDocument.create(redKey, JsonObject.create()
            .put("hex", JsonObject.create()
                .put("red", "ff")
                .put("green", "00")
                .put("blue", "00")
            )));
        assertDocumentRejected(es, CATCH_ALL_INDEX, redKey, "mapper_parsing_exception");

        // Elasticsearch doesn't support BigInteger fields. This error surfaces when creating the index request,
        // before the request is sent to Elasticsearch. Make sure we trapped the error and converted it to a rejection.
        final String bigIntKey = "veryLargeNumber";
        upsertWithRetry(bucket, JsonDocument.create(bigIntKey, JsonObject.create().put("number", new BigInteger("17626319910530664276"))));
        assertDocumentRejected(es, CATCH_ALL_INDEX, bigIntKey, "mapper_parsing_exception");
      }
    }
  }

  /**
   * Verify the type definition that infers index from ID is working as expected.
   */
  private void assertIndexInferredFromDocumentId(Bucket bucket, TestEsClient es) throws Exception {
    upsertWithRetry(bucket, JsonDocument.create("widget::123", JsonObject.create()));
    upsertWithRetry(bucket, JsonDocument.create("widget::foo::bar", JsonObject.create()));
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
         AsyncTask connector = AsyncTask.run(() -> ElasticsearchConnector.run(commonConfig))) {
      waitForTravelSampleReplication(es);
    }
  }
}
