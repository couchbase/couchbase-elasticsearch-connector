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

import com.couchbase.client.core.config.BucketCapabilities;
import com.couchbase.client.core.deps.io.netty.util.ResourceLeakDetector;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.connector.cluster.consul.AsyncTask;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.config.es.ImmutableConnectorConfig;
import com.couchbase.connector.dcp.CouchbaseHelper;
import com.couchbase.connector.elasticsearch.cli.CheckpointClear;
import com.couchbase.connector.testcontainers.CustomCouchbaseContainer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static com.couchbase.client.core.config.BucketCapabilities.COLLECTIONS;
import static com.couchbase.connector.dcp.CouchbaseHelper.forceKeyToPartition;
import static com.couchbase.connector.elasticsearch.ElasticsearchVersionSniffer.Flavor.ELASTICSEARCH;
import static com.couchbase.connector.elasticsearch.ElasticsearchVersionSniffer.Flavor.OPENSEARCH;
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
import static org.junit.Assume.assumeTrue;

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
  private static String cachedSinkContainerVersion;
  private static ElasticsearchVersionSniffer.Flavor cachedSinkFlavor;

  private static CustomCouchbaseContainer couchbase;
  private static SinkContainer sink;
  private static ImmutableConnectorConfig commonConfig;

  @Parameterized.Parameters(name = "cb={0}, sink={2} {1}")
  public static Iterable<Object[]> versionsToTest() {
    // Only the first version in each list will be tested, unless this condition is `true`
    final boolean exhaustive = Boolean.parseBoolean(System.getProperty("com.couchbase.integrationTest.exhaustive"));

    final ImmutableSet<String> couchbaseVersions = ImmutableSet.of(
        "enterprise-7.2.3",
        "community-7.2.2"
    );

    // This list is informed by https://www.elastic.co/support/eol
    // If possible, we also want to support the last release of the previous major version.
    final Set<String> elasticsearchVersions = new LinkedHashSet<>(Arrays.asList(
        "8.12.0", // latest version
        "7.17.16", // latest version of previous major
        "7.14.0" // oldest supported version (first version that sends required "X-Elastic-Product" header)
    ));

    // This list is informed by https://opensearch.org/releases.html
    // If possible, we also want to support the last release of the previous major version.
    final Set<String> opensearchVersions = new LinkedHashSet<>(Arrays.asList(
        "2.11.1", // latest version
        "1.3.14", // latest version of previous major
        "1.3.3" // oldest supported version (first version compatible with opensearch-java client)
    ));

    String newestCouchbase = Iterables.get(couchbaseVersions, 0);
    String newestElasticsearch = Iterables.get(elasticsearchVersions, 0);
    String newestOpenSearch = Iterables.get(opensearchVersions, 0);

    if (!exhaustive) {
      // just test the most recent versions
      return ImmutableList.of(
              new Object[]{newestCouchbase, newestElasticsearch, ELASTICSEARCH},
              new Object[]{newestCouchbase, newestOpenSearch, OPENSEARCH}
      );
    }

    // Full cartesian product is overkill; just test every supported version
    // at least once in some combination.
    final List<Object[]> combinations = new ArrayList<>();

    final Iterable<String> olderCouchbaseVersions = Iterables.skip(couchbaseVersions, 1);

    // Prefer repeating Couchbase versions, since the CB container is more expensive to set up than ES.
    elasticsearchVersions.forEach(es -> combinations.add(new Object[]{newestCouchbase, es, ELASTICSEARCH}));
    opensearchVersions.forEach(os -> combinations.add(new Object[]{newestCouchbase, os, OPENSEARCH}));
    olderCouchbaseVersions.forEach(cb -> combinations.add(new Object[]{cb, newestElasticsearch, ELASTICSEARCH}));

    return combinations;
  }

  private final String couchbaseVersion;
  private final String sinkVersion;
  private final ElasticsearchVersionSniffer.Flavor sinkFlavor;

  public BasicReplicationTest(String couchbaseVersion, String sinkVersion, ElasticsearchVersionSniffer.Flavor flavor) {
    this.couchbaseVersion = requireNonNull(couchbaseVersion);
    this.sinkVersion = requireNonNull(sinkVersion);
    this.sinkFlavor = requireNonNull(flavor);
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

    if (sinkFlavor == cachedSinkFlavor && sinkVersion.equals(cachedSinkContainerVersion)) {
      System.out.println("Using cached sink container " + sink.getDockerImageName() +
          " listening at http://" + sink.getHttpHostAddress());
    } else {
      close(sink);

      sink = SinkContainer.create(sinkFlavor, sinkVersion);
      sink.start();

      System.out.println("Sink listening at http://" + sink.getHttpHostAddress());
      cachedSinkContainerVersion = sinkVersion;
      cachedSinkFlavor = sinkFlavor;
    }

    commonConfig = ConnectorConfig.from(readConfig(couchbase, sink));
    CheckpointClear.clear(commonConfig);

    try (TestEsClient es = new TestEsClient(commonConfig)) {
      es.deleteAllIndexes();
    }
  }

  @AfterClass
  public static void cleanup() throws Exception {
    close(couchbase, sink);
  }

  @Test
  public void createDeleteReject() throws Throwable {
    try (TestCouchbaseClient cb = new TestCouchbaseClient(commonConfig)) {
      final Bucket bucket = cb.createTempBucket(couchbase);
      final Collection collection = bucket.defaultCollection();

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
        final MutationResult upsertResult = upsertWithRetry(bucket, JsonDocument.create(blueKey, JsonObject.create().put("hex", "0000ff")));
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
        assertEquals(upsertResult.cas(), meta.path("cas").longValue());

        MutationToken mutationToken = upsertResult.mutationToken().orElseThrow(() -> new AssertionError("expected mutation token"));
        assertEquals(mutationToken.sequenceNumber(), meta.path("seqno").longValue());
        assertEquals(mutationToken.partitionID(), meta.path("vbucket").longValue());
        assertEquals(mutationToken.partitionUUID(), meta.path("vbuuid").longValue());

        assertEquals("0000ff", content.path("doc").path("hex").textValue());

        // Make sure deletions are propagated to elasticsearch
        collection.remove(blueKey);
        es.waitForDeletion(CATCH_ALL_INDEX, blueKey);

        // Create an incompatible document (different type for "hex" field, Object instead of String)
        final String redKey = "color:red";
        upsertWithRetry(bucket, JsonDocument.create(redKey, JsonObject.create()
            .put("hex", JsonObject.create()
                .put("red", "ff")
                .put("green", "00")
                .put("blue", "00")
            )));
        assertDocumentRejected(es, CATCH_ALL_INDEX, redKey, "_parsing_exception");

        // Elasticsearch doesn't support BigInteger fields. This error surfaces when creating the index request,
        // before the request is sent to Elasticsearch. Make sure we trapped the error and converted it to a rejection.
        final String bigIntKey = "veryLargeNumber";
        upsertWithRetry(bucket, JsonDocument.create(bigIntKey, JsonObject.create().put("number", new BigInteger("17626319910530664276"))));
        assertDocumentRejected(es, CATCH_ALL_INDEX, bigIntKey, "_parsing_exception");
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

    assertFalse("Rejection log message contains sensitive HTTP header",
        content.toString().toLowerCase(Locale.ROOT).contains("authorization"));

    assertEquals(index, content.path("index").textValue());
    assertEquals("INDEX", content.path("action").textValue());
    assertThat(content.path("error").textValue()).contains(reason);
  }

  private void assumeSupportsCollections(TestCouchbaseClient client) {
    assumeTrue("Skipping this test because the server does not support collections.",
        hasCapability(client.cluster().bucket("travel-sample"), COLLECTIONS));
  }

  private static boolean hasCapability(Bucket bucket, BucketCapabilities capability) {
    return CouchbaseHelper.getConfig(bucket.core(), bucket.name())
        .block(Duration.ofMinutes(1))
        .bucketCapabilities()
        .contains(capability);
  }

  @Test
  public void canReplicateTravelSample() throws Throwable {
    try (TestEsClient es = new TestEsClient(commonConfig);
         AsyncTask connector = AsyncTask.run(() -> ElasticsearchConnector.run(commonConfig))) {
      waitForTravelSampleReplication(es);
    }
  }

}
