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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;
import org.elasticsearch.Version;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.connector.dcp.CouchbaseHelper.forceKeyToPartition;
import static com.couchbase.connector.elasticsearch.ElasticsearchHelper.newElasticsearchClient;
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

  private static class TempBucket implements Closeable {
    private final CouchbaseContainer couchbase;
    private final String bucketName;
    private static final AtomicInteger counter = new AtomicInteger();

    public TempBucket(CouchbaseContainer couchbase) {
      this.couchbase = couchbase;
      this.bucketName = "temp-" + counter.getAndIncrement();
      couchbase.createBucket(bucketName);
    }

    @Override
    public void close() {
      couchbase.deleteBucket(bucketName);
    }

    public String name() {
      return bucketName;
    }
  }

  public ImmutableConnectorConfig withBucketName(ImmutableConnectorConfig config, String bucketName) {
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
      ImmutableConnectorConfig config = withBucketName(commonConfig, tempBucket.bucketName);

      try (TestEsClient es = new TestEsClient(config);
           TestConnector connector = new TestConnector(config).start()) {

        final Bucket bucket = cluster.openBucket(tempBucket.name());

        // Create two documents in the same vbucket to make sure we're not conflating seqno and revision number.
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
        assertEquals(doc.cas(), meta.path("cas").longValue());
        assertTrue(meta.path("flags").isIntegralNumber());
        assertEquals(doc.mutationToken().sequenceNumber(), meta.path("seqno").longValue());
        assertEquals(doc.mutationToken().vbucketID(), meta.path("vbucket").longValue());
        assertEquals(doc.mutationToken().vbucketUUID(), meta.path("vbuuid").longValue());

        assertEquals("0000ff", content.path("doc").path("hex").textValue());

        // Make sure deletions are propagated to elasticsearch
        bucket.remove(blueKey);
        es.waitForDeletion("etc", blueKey);

        // Create an incompatible document (different type for "hex" field, Object instead of String)
        bucket.upsert(JsonDocument.create("color:red", JsonObject.create()
            .put("hex", JsonObject.create()
                .put("red", "ff")
                .put("green", "00")
                .put("blue", "00")
            )));
        assertDocumentRejected(es, "etc", "color:red", "mapper_parsing_exception");

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

  private static class TestConnector implements AutoCloseable {
    final Thread thread;
    AtomicReference<Throwable> connectorException = new AtomicReference<>();

    public TestConnector(ConnectorConfig config) {
      thread = new Thread(() -> {
        try {
          ElasticsearchConnector.run(config);
        } catch (Throwable t) {
          if (!(t instanceof InterruptedException)) {
            t.printStackTrace();
          }
          connectorException.set(t);
        }
      });
    }

    public TestConnector start() {
      thread.start();
      return this;
    }

    public void stop() throws Throwable {
      thread.interrupt();
      thread.join(SECONDS.toMillis(30));
      if (thread.isAlive()) {
        throw new TimeoutException("Connector didn't exit in allotted time");
      }
      Throwable t = connectorException.get();
      if (t == null) {
        throw new IllegalStateException("Connector didn't exit by throwing exception");
      }
      if (!(t instanceof InterruptedException)) {
        // The connector failed before we asked it to exit!
        throw t;
      }
    }

    @Override
    public void close() throws Exception {
      try {
        stop();
      } catch (Throwable t) {
        Throwables.propagateIfPossible(t, Exception.class);
        throw new RuntimeException(t);
      }
    }
  }

  private static class TestEsClient implements AutoCloseable {
    private final RestHighLevelClient client;

    public TestEsClient(ConnectorConfig config) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
      this.client = newElasticsearchClient(config.elasticsearch(), config.trustStore());
    }

    @Override
    public void close() throws Exception {
      client.close();
    }

    private JsonNode doGet(String endpoint) throws IOException {
      final Response response = client.getLowLevelClient().performRequest("GET", endpoint);
      try (InputStream is = response.getEntity().getContent()) {
        return new ObjectMapper().readTree(is);
      }
    }

    private long getDocumentCount(String index) {
      try {
        JsonNode response = doGet(index + "/doc/_count");
        return response.get("count").longValue();
      } catch (ResponseException e) {
        return -1;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    private Optional<JsonNode> getDocument(String index, String id) {
      try {
        return Optional.of(doGet(index + "/doc/" + encodeUriPathComponent(id)));

      } catch (ResponseException e) {
        return Optional.empty();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public JsonNode waitForDocument(String index, String id) throws TimeoutException, InterruptedException {
      poll().until(() -> getDocument(index, id).isPresent());
      return getDocument(index, id).get().get("_source");
    }

    public void waitForDeletion(String index, String id) throws TimeoutException, InterruptedException {
      poll().until(() -> !getDocument(index, id).isPresent());
    }
  }

  private static String encodeUriPathComponent(String s) {
    try {
      return URLEncoder.encode(s, "UTF-8").replace("+", "%20");
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError("UTF-8 not supported???", e);
    }
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
