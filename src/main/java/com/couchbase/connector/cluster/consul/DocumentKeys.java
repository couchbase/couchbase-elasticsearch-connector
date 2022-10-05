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

import com.couchbase.client.dcp.core.utils.DefaultObjectMapper;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.BooleanNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.connector.cluster.consul.rpc.Broadcaster;
import com.couchbase.connector.cluster.consul.rpc.RpcEndpoint;
import com.couchbase.connector.cluster.consul.rpc.RpcResult;
import com.couchbase.consul.ConsulOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.couchbase.connector.cluster.consul.ReactorHelper.blockSingle;
import static com.google.common.base.Throwables.propagateIfPossible;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.removeEnd;
import static org.apache.commons.lang3.StringUtils.removeStart;

// This might evolve into a "coordinator" class. For now, it's a mishmash of Consul document keys
// and RPC helper methods.
public class DocumentKeys {
  private static final Logger LOGGER = LoggerFactory.getLogger(DocumentKeys.class);

  private static final Duration DEFAULT_ENDPOINT_TIMEOUT = Duration.ofSeconds(15);

  private final String serviceName;
  private final ConsulOps consul;
  private final ConsulDocumentWatcher watcher;

  public DocumentKeys(ConsulOps consul, ConsulDocumentWatcher watcher, String serviceName) {
    this.consul = requireNonNull(consul);
    this.serviceName = requireNonNull(serviceName);
    this.watcher = requireNonNull(watcher);
  }

  public String root() {
    return "couchbase/cbes/";
  }

  public String config() {
    return serviceKey("config");
  }

  public String control() {
    return serviceKey("control");
  }

  public String leader() {
    return serviceKey("leader");
  }

  private String serviceKey(String suffix) {
    return root() + serviceName + "/" + suffix;
  }

  public String rpcEndpoint(String endpointId) {
    return rpcEndpointKeyPrefix() + requireNonNull(endpointId);
  }

  private String rpcEndpointKeyPrefix() {
    return serviceKey("rpc/");
  }

  private List<String> listKeys(String prefix) {
    return blockSingle(consul.kv().listKeys(prefix, Map.of())).body();
  }

  public List<String> configuredGroups() {
    final String configSuffix = "/config";
    return listKeys(root())
        .stream()
        .filter(s -> s.endsWith(configSuffix))
        .map(s -> removeStart(s, root()))
        .map(s -> removeEnd(s, configSuffix))
        .collect(Collectors.toList());
  }

  public List<RpcEndpoint> listRpcEndpoints(Duration endpointTimeout) {
    requireNonNull(endpointTimeout);
    return listKeys(rpcEndpointKeyPrefix())
        .stream()
        .map(endpointKey -> new RpcEndpoint(consul.kv(), watcher, endpointKey, endpointTimeout))
        .collect(toList());
  }

  public List<RpcEndpoint> listRpcEndpoints() {
    return listRpcEndpoints(DEFAULT_ENDPOINT_TIMEOUT);
  }

  public Optional<RpcEndpoint> leaderEndpoint() {
    final String endpointId = blockSingle(consul.kv().readOneKeyAsString(leader())).orElse(null);
    return endpointId == null ? Optional.empty() : Optional.of(
        new RpcEndpoint(consul.kv(), watcher, rpcEndpoint(endpointId), DEFAULT_ENDPOINT_TIMEOUT));
  }

  public boolean pause() throws TimeoutException, IOException {
    final ObjectNode control = readControlDocument();
    final boolean alreadyPaused = control.path("paused").asBoolean(false);
    if (!alreadyPaused) {
      // todo atomic update?
      control.set("paused", BooleanNode.valueOf(true));
      upsertControlDocument(control);
    }

    waitForClusterToQuiesce(Duration.ofSeconds(30));
    return alreadyPaused;
  }

  private void waitForClusterToQuiesce(Duration timeout) throws TimeoutException {
    final TimeoutEnforcer timeoutEnforcer = new TimeoutEnforcer("Waiting for cluster to quiesce", timeout);
    try (Broadcaster broadcaster = new Broadcaster()) {
      while (true) {
        final List<RpcEndpoint> allEndpoints = listRpcEndpoints();
        final Map<RpcEndpoint, RpcResult<Boolean>> results =
            broadcaster.broadcast("stopped?", allEndpoints, WorkerService.class, WorkerService::stopped);

        boolean stopped = true;
        for (Map.Entry<RpcEndpoint, RpcResult<Boolean>> e : results.entrySet()) {
          if (e.getValue().isFailed()) {
            LOGGER.warn("Status request failed for endpoint " + e.getKey());
            stopped = false;
          } else if (!e.getValue().get()) {
            LOGGER.warn("Endpoint is still working: " + e.getKey());
            stopped = false;
          }
        }
        if (stopped) {
          return;
        }

        timeoutEnforcer.throwIfExpired();
        LOGGER.info("Retrying in just a moment...");
        SECONDS.sleep(2);
      }

    } catch (Exception e) {
      throwIfUnchecked(e);
      propagateIfPossible(e, TimeoutException.class);
      throw new RuntimeException(e);
    }
  }

  private ObjectNode readControlDocument() throws IOException {
    Optional<String> s = blockSingle(consul.kv().readOneKeyAsString(control()));
    return (ObjectNode) DefaultObjectMapper.readTree(blockSingle(consul.kv().readOneKeyAsString(control())).orElse("{}"));
  }

  private void upsertControlDocument(Object value) throws IOException {
    String json = DefaultObjectMapper.writeValueAsString(value);
    boolean success = blockSingle(consul.kv().upsertKey(control(), json)).body();
    if (!success) {
      throw new IOException("Failed to update control document: " + control());
    }
  }

  public void resume() throws IOException {
    // todo atomic update?
    ObjectNode control = readControlDocument();
    final boolean wasPaused = control.path("paused").asBoolean(false);
    if (wasPaused) {
      control.set("paused", BooleanNode.valueOf(false));
      upsertControlDocument(control);
    }
  }
}
