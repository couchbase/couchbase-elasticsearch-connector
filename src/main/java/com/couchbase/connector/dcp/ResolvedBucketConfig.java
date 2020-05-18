/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.connector.dcp;


import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.config.AlternateAddress;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.DefaultNodeInfo;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.dcp.config.HostAndPort;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.couchbase.client.dcp.core.logging.RedactableArgument.system;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * A wrapper around a {@link CouchbaseBucketConfig} that automatically resolves alternate addresses.
 *
 * @implNote This is a trimmed down version of DcpBucketConfig from the DCP client,
 * modified to work with the core-io version of BucketConfig.
 */
public class ResolvedBucketConfig {
  private final boolean sslEnabled;
  private final CouchbaseBucketConfig config;
  private final List<NodeInfo> allNodes;

  public ResolvedBucketConfig(final CouchbaseBucketConfig config, final boolean sslEnabled) {
    this.config = requireNonNull(config);
    this.sslEnabled = sslEnabled;
    this.allNodes = unmodifiableList(resolveAlternateAddresses(config));
  }

  public long rev() {
    return config.rev();
  }

  public String uuid() {
    return config.uuid();
  }

  public int numberOfPartitions() {
    return config.numberOfPartitions();
  }

  public int numberOfReplicas() {
    return config.numberOfReplicas();
  }

  public List<NodeInfo> nodes() {
    return allNodes;
  }

  public List<HostAndPort> getKvAddresses() {
    return allNodes.stream()
        .filter(this::hasKvService)
        .map(this::getKvAddress)
        .collect(Collectors.toList());
  }

  private static List<NodeInfo> resolveAlternateAddresses(CouchbaseBucketConfig config) {
    return config.nodes().stream()
        .map(ResolvedBucketConfig::resolveAlternateAddress)
        .collect(toList());
  }

  private static NodeInfo resolveAlternateAddress(NodeInfo nodeInfo) {
    final String networkName = nodeInfo.useAlternateNetwork();
    if (networkName == null) {
      return nodeInfo; // don't use alternate
    }

    final AlternateAddress alternate = nodeInfo.alternateAddresses().get(networkName);
    if (alternate == null) {
      throw new CouchbaseException("Node " + system(nodeInfo.hostname()) + " has no alternate hostname for network [" + networkName + "]");
    }

    final Map<ServiceType, Integer> services = new HashMap<>(nodeInfo.services());
    final Map<ServiceType, Integer> sslServices = new HashMap<>(nodeInfo.sslServices());
    services.putAll(alternate.services());
    sslServices.putAll(alternate.sslServices());

    return new DefaultNodeInfo(alternate.hostname(), services, sslServices, emptyMap());
  }

  private HostAndPort getKvAddress(final NodeInfo node) {
    int port = getServicePortMap(node).get(ServiceType.BINARY);
    return new HostAndPort(node.hostname(), port);
  }

  private Map<ServiceType, Integer> getServicePortMap(final NodeInfo node) {
    return sslEnabled ? node.sslServices() : node.services();
  }

  private boolean hasKvService(final NodeInfo node) {
    return getServicePortMap(node).containsKey(ServiceType.BINARY);
  }
}
