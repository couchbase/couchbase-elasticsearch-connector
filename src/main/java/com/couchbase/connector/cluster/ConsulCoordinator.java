/*
 * Copyright 2018 Couchbase, Inc.
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

package com.couchbase.connector.cluster;

import com.couchbase.connector.elasticsearch.Metrics;
import com.orbitz.consul.Consul;
import com.orbitz.consul.NotRegisteredException;
import com.orbitz.consul.cache.ServiceHealthCache;
import com.orbitz.consul.model.health.Service;
import com.orbitz.consul.model.health.ServiceHealth;
import com.orbitz.consul.model.session.ImmutableSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.couchbase.connector.util.ThrowableHelper.formatMessageWithStackTrace;
import static com.couchbase.connector.util.ThrowableHelper.hasCause;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

public class ConsulCoordinator implements Coordinator {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsulCoordinator.class);

  private final Consul consul;
  private final String serviceName;
  private final String serviceId;
  private final String serviceUuid = UUID.randomUUID().toString();
  private final String sessionId;
  private final Thread shutdownHook;

  private volatile boolean panic;

  public ConsulCoordinator(String serviceName, String serviceId) {
    this.serviceName = serviceName;
    serviceId = defaultIfNull(serviceId, serviceName);
    this.serviceId = serviceId;

    consul = Consul.builder().build(); // todo configure with credentials / access token
    final List<String> tags = singletonList("couchbase-elasticsearch-connector");// emptyList();
    final Map<String, String> meta = singletonMap("uuid", serviceUuid);

    consul.agentClient().register(0, 30L, this.serviceName, this.serviceId, tags, meta);

    shutdownHook = new Thread(() -> {
      try {
        consul.agentClient().fail(this.serviceId, "(" + this.serviceId + ") Connector process terminated.");
      } catch (Exception e) {
        System.err.println("Failed to report termination to Consul agent.");
        e.printStackTrace();
      }
    });

    Runtime.getRuntime().addShutdownHook(shutdownHook);

    // pass the health check so a session can be created
    try {
      heartbeat();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    this.sessionId = consul.sessionClient().createSession(ImmutableSession.builder()
        .name("couchbase:cbes:" + this.serviceId)
        .behavior("delete")
        .lockDelay("15s")
        .addChecks("service:" + this.serviceId)
        .build()
    ).getId();

    // acquire some locks
    for (int i = 0; i < 1; i++) {
      boolean acquired = consul.keyValueClient().acquireLock("couchbase:cbes:" + serviceName + ":vbucket:" + i, sessionId);
      System.out.println(acquired);
    }
  }

  private static String serviceKey(ServiceHealth health) {
    final Service service = health.getService();
    return String.join("/",
        health.getNode().getAddress(),
        service.getId(),
        service.getMeta().get("uuid"));
  }

  public synchronized void heartbeat() throws ClusterMembershipException {
    if (panic) {
      throw new ClusterMembershipException("Already permanently failed a health check.");
    }

    try {
      consul.agentClient().pass(serviceId, "(" + serviceId + ") OK");

    } catch (NotRegisteredException e) {
      // todo re-register? maybe only if we haven't skipped too many heartbeats since last "pass"?

      if (hasCause(e, ConnectException.class)) {
        throw new ClusterMembershipException("Lost connection to Consul agent.", e);
      }
      throw new ClusterMembershipException("Consul doesn't think we're registered.", e);
    }
  }

  public synchronized Membership updateGroupMembership() throws ClusterMembershipException {
    if (panic) {
      throw new ClusterMembershipException("Already permanently failed a health check.");
    }

    final List<ServiceHealth> healthyServices = consul.healthClient().getHealthyServiceInstances(serviceName).getResponse();
    final List<String> serviceKeys = healthyServices.stream()
        .map(ConsulCoordinator::serviceKey)
        .sorted()
        .collect(toList());

    System.out.println();
    serviceKeys.forEach(System.out::println);

    int i = 1;
    for (String key : serviceKeys) {
      if (key.endsWith(serviceUuid)) {
        return Membership.of(i, serviceKeys.size());
      }
      i++;
    }

    throw new ClusterMembershipException("We're not in the list of healthy services." +
        " Was another connector worker started on this node without specifying a unique service ID?");
  }

  @Override
  public void panic(String message, Throwable t) {
    synchronized (this) {
      panic = true;

      LOGGER.error("PANIC: " + message, t);
      try {
        consul.agentClient().fail(serviceId, formatMessageWithStackTrace(message, t));

      } catch (Exception e) {
        LOGGER.error("Failed to report panic to Consul agent.", e);
      }

      try {
        // We just failed the health check with a specific error message;
        // don't need to fail again with the generic one.
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
      } catch (IllegalStateException e) {
        // Already shutting down? Not a problem.
      }
    }

    // todo think a little harder and exit in a more graceful way
    System.exit(1);
  }

  public static void main(String[] args) throws CoordinatorException, InterruptedException {
    String serviceName = "hoopy-froods";
    String serviceId = null;
    //String serviceId ="again";

    Consul consul = Consul.builder().build();
    //consul.agentClient().deregister(defaultIfNull(serviceId, serviceName));

    ConsulCoordinator coordinator = new ConsulCoordinator(serviceName, serviceId);

    //consul.agentClient().ping();

    // consul.agentClient().getServices()

    System.out.println(consul.agentClient().getMembers());

    ServiceHealthCache svHealth = ServiceHealthCache.newCache(consul.healthClient(), serviceName);

    svHealth.addListener(newValues -> {
      try {
        // System.out.println(consul.agentClient().getMembers());

        System.out.println("new health values: " + coordinator.updateGroupMembership());


      } catch (ClusterMembershipException e) {
        e.printStackTrace();
      }
      //System.out.println("here in listener");
      //System.out.println(newValues);
      // do Something with updated server map
    });
    svHealth.start();

    while (true) {
      SECONDS.sleep(3);
      coordinator.heartbeat();
      // System.out.println(coordinator.updateGroupMembership());

    }
  }
}
