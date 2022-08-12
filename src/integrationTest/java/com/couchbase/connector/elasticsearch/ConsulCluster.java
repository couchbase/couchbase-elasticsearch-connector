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

import com.couchbase.connector.elasticsearch.io.BackoffPolicy;
import com.couchbase.consul.ConsulOps;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static com.couchbase.connector.elasticsearch.DockerHelper.getDockerHost;
import static com.couchbase.connector.elasticsearch.io.BackoffPolicyBuilder.constantBackoff;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;

public class ConsulCluster implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsulCluster.class);

  private static final int HTTP_PORT = 8500;

  private final List<GenericContainer<?>> nodes = new ArrayList<>();

  public ConsulCluster(String dockerImageName, int servers, Network network) {
    checkArgument(servers >= 1);

    for (int i = 0; i < servers; i++) {
      final String nodeName = "consul" + i;
      final boolean first = i == 0;
      final GenericContainer<?> consul = new GenericContainer<>(dockerImageName)
          .withExposedPorts(HTTP_PORT)
          .withNetwork(network)
          .withNetworkAliases(nodeName)
          .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("container." + nodeName)))
          .withCreateContainerCmdModifier(new Consumer<CreateContainerCmd>() {
            @Override
            public void accept(CreateContainerCmd createContainerCmd) {
              final List<String> command = newArrayList(
                  "agent", "-node=" + nodeName, "-client=0.0.0.0", "-bind=0.0.0.0");

              if (servers == 1) {
                command.add("-dev");
              } else {
                command.add("-server");
                command.add("-ui");
                command.add("-bootstrap-expect=" + servers);
                if (!first) {
                  command.add("-join=consul0");
                }
              }
              createContainerCmd.withCmd(command);
            }
          });
      nodes.add(consul);
    }
  }

  public ConsulCluster start() {
    nodes.forEach(GenericContainer::start);
    System.out.println("Consul cluster UI at http://" + getAddress(0));

    System.out.println("Waiting for Consul cluster to elect a leader...");

    final BackoffPolicy backoffPolicy = constantBackoff(Duration.ofSeconds(1)).limit(10).build();
    TestEsClient.retryUntilSuccess(backoffPolicy, () -> {
          try (ConsulOps client = client(nodes.size() - 1)) {
              checkState(client.kv().upsertKey("startup-complete", "true").block().body());
              return null;
          }
        }
    );

    System.out.println("Consul cluster is ready.");

    return this;
  }

  public ConsulCluster stop() {
    LOGGER.info("Shutting down Consul cluster...");
    nodes.forEach(GenericContainer::stop);
    LOGGER.info("Consul cluster shutdown complete.");
    return this;
  }

  public String getAddress(int nodeIndex) {
    return getDockerHost() + ":" + nodes.get(nodeIndex).getMappedPort(HTTP_PORT);
  }

  public HostAndPort getHostAndPort(int nodeIndex) {
    GenericContainer<?> node = nodes.get(nodeIndex);
    return HostAndPort.fromParts(node.getHost(), node.getMappedPort(8500));
  }

  private ConsulOps client(int nodeIndex) {
    return new ConsulOps(HostAndPort.fromString(getAddress(nodeIndex)), ConsulOps.DEFAULT_CONFIG);
  }

  @Override
  public void close() {
    stop();
  }
}
