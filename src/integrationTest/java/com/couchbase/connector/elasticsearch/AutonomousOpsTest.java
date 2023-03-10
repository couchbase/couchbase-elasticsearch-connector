/*
 * Copyright 2019 Couchbase, Inc.
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

import com.couchbase.client.core.deps.io.netty.util.ResourceLeakDetector;
import com.couchbase.client.java.Bucket;
import com.couchbase.connector.testcontainers.CustomCouchbaseContainer;
import com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.Network;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.couchbase.connector.elasticsearch.BasicReplicationTest.CATCH_ALL_INDEX;
import static com.couchbase.connector.elasticsearch.ElasticsearchVersionSniffer.Flavor.ELASTICSEARCH;
import static com.couchbase.connector.elasticsearch.IntegrationTestHelper.close;
import static com.couchbase.connector.elasticsearch.IntegrationTestHelper.upsertOneDocumentToEachVbucket;
import static com.couchbase.connector.elasticsearch.IntegrationTestHelper.waitForTravelSampleReplication;
import static com.couchbase.connector.elasticsearch.TestConfigHelper.readConfig;
import static com.couchbase.connector.testcontainers.CustomCouchbaseContainer.newCouchbaseCluster;
import static com.couchbase.connector.testcontainers.Poller.poll;
import static java.util.concurrent.TimeUnit.SECONDS;

public class AutonomousOpsTest {

  static final String CONSUL_DOCKER_IMAGE = "consul:1.15.1";

  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    TestConnectorGroup.buildConnector();
  }

  private static final String couchbaseVersion = "enterprise-7.1.1";
  private static final String elasticsearchVersion = "7.17.9";

  private static CustomCouchbaseContainer couchbase;
  private static SinkContainer sink;
  private static ConsulCluster consulCluster;

  @BeforeClass
  public static void startReusableContainers() {
    consulCluster = new ConsulCluster(CONSUL_DOCKER_IMAGE, 3, Network.newNetwork()).start();

    sink = SinkContainer.create(ELASTICSEARCH, elasticsearchVersion);
    sink.start();

    couchbase = newCouchbaseCluster("couchbase/server:" + couchbaseVersion);

    System.out.println("Couchbase " + couchbase.getVersionString() +
        " listening at http://" + DockerHelper.getDockerHost() + ":" + couchbase.getMappedPort(8091));
  }

  @AfterClass
  public static void stopReusableContainers() {
    close(couchbase, sink, consulCluster);
  }

  @Before
  public void resetElasticsearch() throws Exception {
    try (TestEsClient es = new TestEsClient(defaultConfig())) {
      es.deleteAllIndexes();
    }
  }

  private static final AtomicInteger nameCounter = new AtomicInteger();

  private static String newGroupName() {
    return "integrationTest-" + nameCounter.incrementAndGet();
  }

  public String defaultConfig() throws IOException {
    return defaultConfig("travel-sample");
  }

  public String defaultConfig(String bucketName) throws IOException {
    return readConfig(couchbase, sink, ImmutableMap.of(
        "group.name", newGroupName(),
        "couchbase.bucket", bucketName));
  }

  @Test
  public void singleWorker() throws Exception {
    couchbase.loadSampleBucket("travel-sample", 100);

    final String config = defaultConfig();

    try (TestConnectorGroup group = new TestConnectorGroup(config, consulCluster);
         TestEsClient es = new TestEsClient(config)) {
      group.newWorker(0);
      group.awaitRebalance(1);
      waitForTravelSampleReplication(es);
    }
  }

  @Test
  public void threeWorkers() throws Exception {
    final String bucketName = TempBucket.nextName();
    final String config = defaultConfig(bucketName);

    try (TestEsClient es = new TestEsClient(config);
         TestCouchbaseClient cb = new TestCouchbaseClient(config);
         TestConnectorGroup group = new TestConnectorGroup(config, consulCluster)) {

      final Bucket bucket = cb.createTempBucket(couchbase, bucketName);
      group.pause();

      // Wait for first worker to assume role of leader
      final Closeable worker1 = group.newWorker(0);
      poll().until(() -> group.leaderEndpoint().isPresent());

      final Closeable worker2 = group.newWorker(1);
      final Closeable worker3 = group.newWorker(2);

      // Wait for all instances to bind to RPC endpoints
      group.awaitBoundEndpoints(3);

      // Membership is assigned by the leader when the leader tells everyone to start streaming.
      // Nobody should be streaming yet, since the group is paused.
      SECONDS.sleep(2);
      group.assertNobodyStreaming();

      // Allow streaming to begin
      group.resume();
      group.awaitRebalance(3);
      es.waitForDocuments(CATCH_ALL_INDEX, upsertOneDocumentToEachVbucket(bucket, "a"));

      System.out.println("Stopping leader");
      worker1.close();
      System.out.println("Leader stopped!");

      // Wait for new leader to take over and rebalance among remaining workers
      group.awaitRebalance(2);
      es.waitForDocuments(CATCH_ALL_INDEX, upsertOneDocumentToEachVbucket(bucket, "b"));

      System.out.println("Adding 'latecomer' worker");
      final Closeable connector4 = group.newWorker(2, "latecomer");

      // new node should be integrated into group
      group.awaitRebalance(3);
      es.waitForDocuments(CATCH_ALL_INDEX, upsertOneDocumentToEachVbucket(bucket, "c"));

      // stop a node that isn't the leader
      System.out.println("Stopping 'latecomer' worker");
      connector4.close();
      System.out.println("Latecomer worker stopped!");

      // leaver should be removed from the group
      group.awaitRebalance(2);
      es.waitForDocuments(CATCH_ALL_INDEX, upsertOneDocumentToEachVbucket(bucket, "d"));

      System.out.println("Shutting down connector...");
    }
    System.out.println("Connector shutdown complete.");
  }
}
