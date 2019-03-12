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

import com.couchbase.client.deps.io.netty.util.ResourceLeakDetector;
import com.couchbase.connector.testcontainers.CustomCouchbaseContainer;
import com.couchbase.connector.testcontainers.ElasticsearchContainer;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.Version;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.couchbase.connector.elasticsearch.IntegrationTestHelper.close;
import static com.couchbase.connector.elasticsearch.IntegrationTestHelper.waitForTravelSampleReplication;
import static com.couchbase.connector.elasticsearch.TestConfigHelper.readConfig;
import static com.couchbase.connector.testcontainers.CustomCouchbaseContainer.newCouchbaseCluster;
import static com.couchbase.connector.testcontainers.Poller.poll;
import static java.util.concurrent.TimeUnit.SECONDS;

public class AutonomousOpsTest {
  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  private static final String couchbaseVersion = "enterprise-6.0.1";
  private static final String elasticsearchVersion = "6.6.0";

  private static CustomCouchbaseContainer couchbase;
  private static ElasticsearchContainer elasticsearch;
  private static ConsulCluster consulCluster;

  @BeforeClass
  public static void startReusableContainers() {
    consulCluster = new ConsulCluster("consul:1.4.3", 3, Network.newNetwork()).start();

    elasticsearch = new ElasticsearchContainer(Version.fromString(elasticsearchVersion))
        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("container.elasticsearch")));
    elasticsearch.start();

    couchbase = newCouchbaseCluster("couchbase/server:" + couchbaseVersion);
    couchbase.loadSampleBucket("travel-sample", 100);

    System.out.println("Couchbase " + couchbase.getVersionString() +
        " listening at http://" + DockerHelper.getDockerHost() + ":" + couchbase.getMappedPort(8091));
  }

  @AfterClass
  public static void stopReusableContainers() {
    close(couchbase, elasticsearch, consulCluster);
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
    return readConfig(couchbase, elasticsearch, ImmutableMap.of(
        "group.name", newGroupName()));
  }

  @Test
  public void singleWorker() throws Exception {
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
    final String config = defaultConfig();

    try (TestConnectorGroup group = new TestConnectorGroup(config, consulCluster);
         TestEsClient es = new TestEsClient(config)) {

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

      SECONDS.sleep(2);
      System.out.println("Stopping leader");
      worker1.close();
      System.out.println("Leader stopped!");

      // Wait for new leader to take over and rebalance among remaining workers
      group.awaitRebalance(2);
      SECONDS.sleep(8);

      final Closeable connector4 = group.newWorker(2, "latecomer");
      // new node should be integrated into group
      group.awaitRebalance(3);

      waitForTravelSampleReplication(es);

      System.out.println("Shutting down connector...");
    }
    System.out.println("Connector shutdown complete.");
  }
}
