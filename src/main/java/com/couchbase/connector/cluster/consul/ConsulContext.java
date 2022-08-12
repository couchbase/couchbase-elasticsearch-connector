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

package com.couchbase.connector.cluster.consul;

import com.couchbase.connector.cluster.consul.rpc.RpcEndpoint;
import com.couchbase.connector.config.ConfigException;
import com.couchbase.connector.config.common.ConsulConfig;
import com.couchbase.consul.ConsulOps;
import com.couchbase.consul.ConsulResponse;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.couchbase.connector.cluster.consul.ReactorHelper.blockSingle;
import static java.util.Objects.requireNonNull;

public class ConsulContext implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(ConsulContext.class);

  private final DocumentKeys keys;
  private final String serviceName;
  private final String serviceId;
  private final String serviceUuid = UUID.randomUUID().toString();
  private final ConsulDocumentWatcher documentWatcher;
  private final ConsulOps client;
  private final String ttlCheckName;
  private final ConsulConfig config;
  private final HostAndPort address;

  public ConsulContext(HostAndPort address, ConsulConfig consulConfig, String serviceName, @Nullable String serviceIdOrNull) {
    this.config = requireNonNull(consulConfig);
    this.serviceName = requireNonNull(serviceName);
    this.serviceId = Optional.ofNullable(serviceIdOrNull).orElse(serviceName);
    this.client = new ConsulOps(address, consulConfig);
    this.ttlCheckName = "service:" + serviceId;

    this.documentWatcher = new ConsulDocumentWatcher(client);
    this.keys = new DocumentKeys(client, this.documentWatcher, serviceName);
    this.address = requireNonNull(address);
  }

  public boolean pause() throws IOException, TimeoutException {
    return keys.pause();
  }

  public void resume() throws IOException {
    keys.resume();
  }

  public ConsulOps client() {
    return client;
  }

  public DocumentKeys keys() {
    return keys;
  }

  public List<RpcEndpoint> rpcEndpoints() {
    return keys.listRpcEndpoints();
  }

  public String readConfig() {
    log.info("Reading connector config from Consul key: {}", keys.config());

    return blockSingle(client.kv().readOneKeyAsString(keys.config()))
        .orElseThrow(() -> new ConfigException("Connector config does not exist in Consul. Missing KV key: " + keys.config()));
  }

  public <T> T readConfigOrExit(Function<String, T> configParser) {
    try {
      final String configString = readConfig();
      if (Strings.isNullOrEmpty(configString)) {
        System.err.println("ERROR: Connector configuration document does not exist, or is empty.");
        System.exit(1);
      }

      try {
        return configParser.apply(configString);
      } catch (Exception e) {
        System.err.println("ERROR: Connector configuration document is malformed.");
        e.printStackTrace();
        System.exit(1);
        throw new AssertionError("unreachable");
      }
    } catch (Exception e) {
      System.err.println("ERROR: Failed to read connector configuration document.");
      e.printStackTrace();
      System.exit(1);
      throw new AssertionError("unreachable");
    }
  }

  public String serviceName() {
    return serviceName;
  }

  public String serviceId() {
    return serviceId;
  }

  public void register(Duration healthCheckTtl) {
    client.agent()
        .register(
            Map.of(
                "Id", serviceId,
                "Name", serviceName,
                "Meta", Map.of("uuid", serviceUuid),
                "Tags", List.of("couchbase-elasticsearch-connector"),
                "Check", Map.of(
                    "CheckID", ttlCheckName,
                    "TTL", healthCheckTtl.toSeconds() + "s",
                    "DeregisterCriticalServiceAfter", config.deregisterCriticalServiceAfter().toSeconds() + "s"
                )
            ),
            Map.of("replace-existing-checks", "")
        )
        .block();
  }

  public String createSession() {
    ConsulResponse<String> sessionResponse = blockSingle(client.session()
        .createSession(
            Map.of(
                "Name", "couchbase:cbes:" + serviceId,
                "Behavior", "delete",
                "LockDelay", "15s",
                "Checks", List.of(
                    "serfHealth", // Must include "serfHealth", otherwise session never expires if Consul agent fails.
                    "service:" + serviceId
                )
            ),
            Map.of()
        ));

    return sessionResponse.body();
  }

  public void passHealthCheck() {
    client.agent()
        .check("pass", ttlCheckName, Map.of("note", "(" + serviceId + ") OK"))
        .block();
  }

  private void failHealthCheck(String message) {
    client.agent()
        .check("fail", ttlCheckName, Map.of("note", "(" + serviceId + ") " + message))
        .block();
  }

  private void deregister() {
    client.agent()
        .deregister(serviceId)
        .block();
  }

  public Flux<Optional<String>> watchConfig() {
    return documentWatcher.watch(keys().config());
  }

  public Flux<Optional<String>> watchControl() {
    return documentWatcher.watch(keys().control());
  }

  public ConsulDocumentWatcher documentWatcher() {
    return documentWatcher;
  }

  public Flux<ImmutableSet<String>> watchServiceHealth(Duration quietPeriod) {
    return ConsulHelper.watchServiceHealth(client, serviceName, quietPeriod);
  }

  @Override
  public void close() {
    client.close();
  }

  // Useful for cleanup code that should not be interrupted.
  public void runCleanup(Runnable cleanupTask) {
    final AtomicReference<Throwable> deferred = new AtomicReference<>();
    final Thread thread = new Thread(() -> {
      try {
        cleanupTask.run();
      } catch (Throwable t) {
        deferred.set(t);
      }
    });
    thread.start();

    final int timeoutSeconds = 30;
    Uninterruptibles.joinUninterruptibly(thread, timeoutSeconds, TimeUnit.SECONDS);
    if (thread.isAlive()) {
      throw new IllegalStateException("cleanup thread failed to complete within " + timeoutSeconds + " seconds.");
    }

    final Throwable t = deferred.get();
    if (t != null) {
      Throwables.throwIfUnchecked(t);
      throw new RuntimeException(t);
    }
  }

  public boolean acquireLock(String key, String value, String sessionId) {
    return blockSingle(
        client.kv().upsertKey(key, value, Map.of("acquire", sessionId))
    ).body();
  }

  public boolean unlockAndDelete(String key, String sessionId) {
    return ConsulHelper.unlockAndDelete(client.httpClient(), key, sessionId);
  }

  public String myEndpointId() {
    ObjectNode self = blockSingle(client.agent().self()).body();
    return formatEndpointId(
        requireNonNull(self.path("Member").path("Name").textValue(), "missing Member.Name"),
        requireNonNull(self.path("Member").path("Addr").textValue(), "missing Member.Addr"),
        serviceId
    );
  }

  public static String formatEndpointId(String nodeName, String nodeAddress, String serviceId) {
    return String.join("::", nodeName, nodeAddress, serviceId);
  }

  public void reportShutdown(@Nullable Throwable cause) {
    try {
      if (cause != null) {
        failHealthCheck("Shutdown triggered by exception: " + Throwables.getStackTraceAsString(cause));
        return;
      }

      if (config.deregisterServiceOnGracefulShutdown()) {
        deregister();
        return;
      }

      failHealthCheck("Graceful shutdown complete.");

    } catch (Throwable t) {
      // Logging framework is likely shut down by now, so write directly to console.
      System.err.println("Failed to report termination to Consul agent.");
      if (cause != null) {
        t.addSuppressed(cause);
      }
      t.printStackTrace();
    }
  }

  public String consulAddress() {
    return address.toString();
  }
}
