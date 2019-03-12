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
import com.couchbase.connector.cluster.consul.AsyncTask;
import com.couchbase.connector.cluster.consul.ConsulConnector;
import com.couchbase.connector.cluster.consul.ConsulContext;
import com.couchbase.connector.cluster.consul.WorkerService;
import com.couchbase.connector.cluster.consul.rpc.RpcEndpoint;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.google.common.io.Closer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.couchbase.connector.testcontainers.Poller.poll;
import static java.util.Collections.singleton;
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
    this.ctx = closer.register(new ConsulContext(consulCluster.clientBuilder(0), groupName, null));

    configure(config);
  }

  public Closeable newWorker(int consulAgentIndex) {
    return newWorker(consulAgentIndex, null);
  }

  public Closeable newWorker(int consulAgentIndex, String serviceId) {
    return closer.register(AsyncTask.run(() -> ConsulConnector.run(
        new ConsulContext(consulCluster.clientBuilder(consulAgentIndex), groupName, serviceId))));
  }

  public void pause() throws TimeoutException, IOException {
    ctx.keys().pause();
  }

  public void resume() throws IOException {
    ctx.keys().resume();
  }

  public void configure(String config) {
    ctx.consul().keyValueClient()
        .putValue(ctx.keys().config(), config);
  }

  public List<RpcEndpoint> endpoints() {
    return ctx.keys().listRpcEndpoints();
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
