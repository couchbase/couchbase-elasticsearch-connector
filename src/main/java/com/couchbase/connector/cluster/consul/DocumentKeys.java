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

import com.couchbase.client.core.utils.DefaultObjectMapper;
import com.couchbase.connector.cluster.consul.rpc.Broadcaster;
import com.couchbase.connector.cluster.consul.rpc.RpcEndpoint;
import com.couchbase.connector.cluster.consul.rpc.RpcResult;
import com.google.common.collect.ImmutableMap;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.model.agent.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.google.common.base.Throwables.propagateIfPossible;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.nio.charset.StandardCharsets.UTF_8;
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
  private KeyValueClient kv;

  public DocumentKeys(KeyValueClient kv, String serviceName) {
    this.serviceName = requireNonNull(serviceName);
    this.kv = requireNonNull(kv);
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

  public List<String> configuredGroups() {
    final String configSuffix = "/config";
    return ConsulHelper.listKeys(kv, root())
        .stream()
        .filter(s -> s.endsWith(configSuffix))
        .map(s -> removeStart(s, root()))
        .map(s -> removeEnd(s, configSuffix))
        .collect(Collectors.toList());
  }

  public List<RpcEndpoint> listRpcEndpoints(Duration endpointTimeout) {
    requireNonNull(endpointTimeout);
    return ConsulHelper.listKeys(kv, rpcEndpointKeyPrefix())
        .stream()
        .map(endpointKey -> new RpcEndpoint(kv, endpointKey, endpointTimeout))
        .collect(toList());
  }

  public List<RpcEndpoint> listRpcEndpoints() {
    return listRpcEndpoints(DEFAULT_ENDPOINT_TIMEOUT);
  }

  public Optional<RpcEndpoint> leaderEndpoint() {
    final String endpointId = kv.getValueAsString(leader(), UTF_8).orElse(null);
    return endpointId == null ? Optional.empty() : Optional.of(
        new RpcEndpoint(kv, rpcEndpoint(endpointId), DEFAULT_ENDPOINT_TIMEOUT));
  }

  // consul-specific
  public static String endpointId(Member member, String serviceId) {
    return member.getName() + "::" + member.getAddress() + "::" + serviceId;
  }

  public boolean pause() throws TimeoutException, IOException {
    final String previous = kv.getValueAsString(control(), UTF_8).orElse("{}");
    final boolean alreadyPaused = DefaultObjectMapper.readTree(previous).path("paused").asBoolean(false);
    if (!alreadyPaused) {
      final String newValue = DefaultObjectMapper.writeValueAsString(ImmutableMap.of("paused", true));
      final boolean success = kv.putValue(control(), newValue, UTF_8);
      if (!success) {
        throw new IOException("Failed to update control document: " + control());
      }
    }

    waitForClusterToQuiesce(Duration.ofSeconds(30));
    return alreadyPaused;
  }

  private void waitForClusterToQuiesce(Duration timeout) throws TimeoutException {
    final TimeoutEnforcer timeoutEnforcer = new TimeoutEnforcer(timeout);
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

  public boolean resume() throws IOException {
    final String previous = kv.getValueAsString(control(), UTF_8).orElse("{}");
    final boolean wasPaused = DefaultObjectMapper.readTree(previous).path("paused").asBoolean(false);
    if (!wasPaused) {
      return false;
    }

    final String newValue = DefaultObjectMapper.writeValueAsString(ImmutableMap.of("paused", false));
    final boolean success = kv.putValue(control(), newValue, UTF_8);
    if (!success) {
      throw new IOException("Failed to update control document: " + control());
    }
    return true;
  }
}
