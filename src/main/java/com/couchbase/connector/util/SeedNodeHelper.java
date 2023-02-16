/*
 * Copyright 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connector.util;

import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.config.AlternateAddress;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.ConnectionString;
import com.couchbase.client.core.util.ConnectionStringUtil;
import com.couchbase.client.dcp.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.connector.dcp.CouchbaseHelper;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.couchbase.client.core.service.ServiceType.KV;
import static com.couchbase.client.core.service.ServiceType.MANAGER;
import static com.couchbase.client.dcp.core.logging.RedactableArgument.system;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toSet;

public class SeedNodeHelper {
  private SeedNodeHelper() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Returns complete host and port information for every node running the KV service.
   */
  public static Set<SeedNode> getKvNodes(Bucket bucket, ConnectionString connectionString, boolean tls, NetworkResolution networkResolution, Duration timeout) {
    // Bucket config is most reliable way to determine KV ports
    BucketConfig config = CouchbaseHelper.getConfig(bucket, timeout);

    // Alternate address inference (NetworkResolution.AUTO) requires first resolving DNS SRV records
    // so the actual node names can be used.
    if (networkResolution.equals(NetworkResolution.AUTO)) {
      EventBus eventBus = bucket.environment().eventBus();

      // The Java client calls seedNodesFromConnectionString during bootstrap, but the results
      // are not accessible here. Call it again.
      Set<String> seedHosts = ConnectionStringUtil.seedNodesFromConnectionString(connectionString, true, tls, eventBus)
          .stream()
          .map(SeedNode::address)
          .collect(toSet());

      networkResolution = inferNetwork(config, networkResolution, seedHosts);
    }

    // Resolve alternate addresses
    NetworkResolution finalNetworkResolution = networkResolution; // so lambda can access it
    return config.nodes()
        .stream()
        .map(node -> new ResolvedNodeInfo(node, tls, finalNetworkResolution))
        .map(node -> SeedNode.create(node.host(), node.port(KV), node.port(MANAGER)))
        .filter(node -> node.kvPort().isPresent())
        .collect(toSet());
  }

  /**
   * The result of deciding which network to use and whether to use TLS.
   */
  private static class ResolvedNodeInfo {
    private final String host;
    private final Map<ServiceType, Integer> services;

    public ResolvedNodeInfo(NodeInfo unresolved, boolean tls, NetworkResolution networkResolution) {
      if (networkResolution.equals(NetworkResolution.AUTO)) {
        throw new IllegalArgumentException("Must resolve 'auto' network first");
      }

      if (networkResolution.equals(NetworkResolution.DEFAULT)) {
        this.host = unresolved.hostname();
        this.services = unmodifiableMap(new HashMap<>(services(unresolved, tls)));

      } else {
        final AlternateAddress alternate = unresolved.alternateAddresses().get(networkResolution.name());
        if (alternate == null) {
          throw new CouchbaseException("Node " + system(unresolved.hostname()) + " has no alternate hostname for network [" + networkResolution + "]");
        }

        this.host = alternate.hostname();

        // Alternate might not override all ports, so overlay on top of default ports
        Map<ServiceType, Integer> tempServices = new HashMap<>(services(unresolved, tls));
        tempServices.putAll(services(alternate, tls));
        this.services = unmodifiableMap(tempServices);
      }
    }

    public String host() {
      return host;
    }

    public Optional<Integer> port(ServiceType serviceType) {
      return Optional.ofNullable(services.get(serviceType));
    }
  }

  private static Map<ServiceType, Integer> services(NodeInfo node, boolean secure) {
    return secure ? node.sslServices() : node.services();
  }

  private static Map<ServiceType, Integer> services(AlternateAddress node, boolean secure) {
    return secure ? node.sslServices() : node.services();
  }

  private static NetworkResolution inferNetwork(final BucketConfig config, final NetworkResolution network,
                                                final Set<String> seedHosts) {
    if (!network.equals(NetworkResolution.AUTO)) {
      return network;
    }

    for (NodeInfo info : config.nodes()) {
      if (seedHosts.contains(info.hostname())) {
        return NetworkResolution.DEFAULT;
      }

      Map<String, AlternateAddress> aa = info.alternateAddresses();
      if (aa != null && !aa.isEmpty()) {
        for (Map.Entry<String, AlternateAddress> entry : aa.entrySet()) {
          AlternateAddress alternateAddress = entry.getValue();
          if (alternateAddress != null && seedHosts.contains(alternateAddress.hostname())) {
            return NetworkResolution.valueOf(entry.getKey());
          }
        }
      }
    }

    return NetworkResolution.DEFAULT;
  }
}
