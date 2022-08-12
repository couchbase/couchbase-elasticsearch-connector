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

import com.couchbase.connector.cluster.Membership;
import com.couchbase.connector.cluster.consul.ConsulContext;
import com.couchbase.connector.cluster.consul.WorkerService;
import com.couchbase.connector.cluster.consul.rpc.RpcEndpoint;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.consul.ConsulOps;
import com.google.common.io.Closer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.couchbase.connector.cluster.consul.ReactorHelper.blockSingle;
import static com.couchbase.connector.testcontainers.Poller.poll;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

class TestConnectorGroup implements Closeable {
  private final String groupName;
  private final ConsulCluster consulCluster;
  private final Closer closer = Closer.create();
  private final ConsulContext ctx;

  public TestConnectorGroup(String config, ConsulCluster consulCluster) {
    final ConnectorConfig parsedConfig = ConnectorConfig.from(config);
    this.groupName = parsedConfig.group().name();
    this.consulCluster = consulCluster;
    this.ctx = closer.register(new ConsulContext(
        consulCluster.getHostAndPort(0),
        ConsulOps.DEFAULT_CONFIG,
        groupName,
        null
    ));

    configure(config);
  }

  public Closeable newWorker(int consulAgentIndex) {
    return newWorker(consulAgentIndex, null);
  }

  private static String startScript =
      "build/install/couchbase-elasticsearch-connector/bin/cbes-consul";

  private static volatile boolean builtConnector = false;

  public static void buildConnector() {
    try {
      System.out.println("Making sure connector installation is up to date...");
      Process p = new ProcessBuilder("sh", "-c", "./gradlew install")
          .inheritIO()
          .start();
      if (p.waitFor() != 0) {
        throw new RuntimeException("gradle failed with exit value " + p.exitValue());
      }

      // sanity check
      if (!new File(startScript).exists()) {
        throw new RuntimeException("Expected installed connector binary, but nothing found at " + startScript);
      }

      builtConnector = true;

    } catch (Exception e) {
      throw new RuntimeException("failed to build connector installation", e);
    }
  }

  /**
   * @implNote Launches a separate process, so we can easily and completely shut down the
   * process by sending it an interrupt signal, the same way a user would.
   */
  public Closeable newWorker(int consulAgentIndex, String serviceId) {
    checkState(builtConnector, "Must call buildConnector() first");

    AtomicBoolean closeRequested = new AtomicBoolean();
    try {
      String consulAddress = consulCluster.getHostAndPort(consulAgentIndex).toString();

      String command = startScript + " run" +
          " --consul-agent=" + consulAddress +
          " --group=" + groupName;
      if (serviceId != null) {
        command += " --service-id=" + serviceId;
      }

      Process p = new ProcessBuilder("sh", "-c", command)
          .inheritIO()
          .start();
      long pid = p.pid();

      p.onExit().whenComplete((process, throwable) -> {
        if (!closeRequested.get()) {
          System.err.println("Connector process " + pid + " died unexpectedly with exit code " + p.exitValue());
          System.exit(1);
        }
      });

      System.out.println("Started connector process " + pid + " with command: " + command);

      Closeable c = () -> {
        closeRequested.set(true);
        System.out.println("Destroying connector process " + pid + " (Consul agent " + consulAddress + ")");
        p.destroy();
        try {
          if (!p.waitFor(10, SECONDS)) {
            System.err.println("Connector process " + pid + " failed to exit promptly.");
            System.exit(1);
          }
          System.out.println("Connector processes " + pid + " exited with status code: " + p.exitValue());
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      };
      return closer.register(c);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void pause() throws TimeoutException, IOException {
    ctx.pause();
  }

  public void resume() throws IOException {
    ctx.resume();
  }

  public void configure(String config) {
    blockSingle(ctx.client().kv().upsertKey(ctx.keys().config(), config));
  }

  public List<RpcEndpoint> endpoints() {
    return ctx.rpcEndpoints();
  }

  public Optional<RpcEndpoint> leaderEndpoint() {
    return ctx.keys().leaderEndpoint();
  }

  public void awaitBoundEndpoints(int endpointCount) throws TimeoutException, InterruptedException {
    poll().until(() -> endpoints().size() == endpointCount);
  }

  public void awaitRebalance(int expectedGroupSize) throws TimeoutException, InterruptedException {
    System.out.println("Waiting for group to have " + expectedGroupSize + " streaming members.");

    final Set<Membership> expectedMemberships = IntStream.range(0, expectedGroupSize)
        .mapToObj(i -> Membership.of(i + 1, expectedGroupSize))
        .collect(toSet());

    poll().until(() -> getMemberships().equals(expectedMemberships));

    System.out.println("Group rebalanced with " + expectedGroupSize + " streaming members.");
  }

  public void assertNobodyStreaming() {
    assertEquals(singleton(null), getMemberships());
  }

  public Set<Membership> getMemberships() {
    return endpoints().stream()
        .map(ep -> ep.service(WorkerService.class).status().getMembership())
        .collect(Collectors.toSet());
  }

  @Override
  public void close() throws IOException {
    closer.close();
  }
}
