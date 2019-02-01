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
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.model.agent.Member;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.removeEnd;
import static org.apache.commons.lang3.StringUtils.removeStart;

// This might evolve into a "coordinator" class. For now, it's a mishmash of Consul document keys
// and RPC helper methods.
public class DocumentKeys {
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
}
