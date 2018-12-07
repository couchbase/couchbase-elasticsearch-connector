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

package com.couchbase.connector.cluster.consul;

import com.couchbase.client.core.logging.RedactableArgument;
import com.couchbase.connector.util.QuietPeriodExecutor;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.orbitz.consul.Consul;
import com.orbitz.consul.cache.ServiceHealthCache;
import com.orbitz.consul.model.health.ServiceHealth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.couchbase.connector.cluster.consul.ConsulHelper.listRpcEndpoints;
import static com.couchbase.connector.dcp.DcpHelper.allPartitions;
import static com.couchbase.connector.util.ListHelper.chunks;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LeaderTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeaderTask.class);

  private static final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
  private static final QuietPeriodExecutor quietPeriodExecutor = new QuietPeriodExecutor(Duration.ofSeconds(5), executorService);


  private Consul consul;
  private String serviceName;
  private int numVbuckets;
  private volatile boolean done;
  private volatile Thread thread;

  public LeaderTask(Consul consul, String serviceName, int numVbuckets) {
    checkArgument(numVbuckets > 0);

    this.consul = requireNonNull(consul);
    this.serviceName = requireNonNull(serviceName);
    this.numVbuckets = numVbuckets;
  }

  public LeaderTask start() {
    checkState(thread == null, "Already started.");
    thread = new Thread(this::doRun);
    thread.start();
    return this;
  }

  private volatile ImmutableSet<String> prevEndpointIds = ImmutableSet.of();


  private static String endpointId(ServiceHealth health) {
    return health.getNode().getNode() + "::" + health.getNode().getAddress() + "::" + health.getService().getId();
  }

  private void doRun() {
    final ServiceHealthCache svHealth = ServiceHealthCache.newCache(consul.healthClient(), serviceName);

    try {
      final BlockingQueue<String> eventQueue = new LinkedBlockingQueue<>();

      svHealth.addListener(newValues -> {
        final ImmutableSet<String> currentEndpointIds = ImmutableSet.copyOf(newValues.values().stream().map(LeaderTask::endpointId).collect(Collectors.toSet()));
        if (!currentEndpointIds.equals(prevEndpointIds)) {

          if (LOGGER.isInfoEnabled()) {
            final Set<String> joiningNodes = Sets.difference(currentEndpointIds, prevEndpointIds);
            final Set<String> leavingNodes = Sets.difference(prevEndpointIds, currentEndpointIds);
            LOGGER.info("Service health changed; will rebalance after quiet period. Joining: {} Leaving: {}", joiningNodes, leavingNodes);
          }

          prevEndpointIds = currentEndpointIds;
          quietPeriodExecutor.schedule(() -> eventQueue.add("group membership changed"));
        }
      });
      svHealth.start();

      while (true) {
        throwIfDone();
        eventQueue.take();
        rebalance();
      }

    } catch (InterruptedException e) {

    } finally {
      svHealth.stop();
    }

  }


  private <T> Map<RpcEndpoint, T> broadcast(Function<RpcEndpoint, T> endpointCallback) {
    final List<RpcEndpoint> endpoints = listRpcEndpoints(consul.keyValueClient(), serviceName, Duration.ofSeconds(15));

    // todo invoke in parallel so timeouts don't stack
    final Map<RpcEndpoint, T> results = new HashMap<>();
    for (RpcEndpoint endpoint : endpoints) {
      try {
        results.put(endpoint, endpointCallback.apply(endpoint));
      } catch (Exception e) {
        LOGGER.error("Failed to apply callback for endpoint {}", RedactableArgument.system(endpoint), e);
      }
    }
    return results;
  }

  private <S, T> Map<RpcEndpoint, RpcResult<T>> broadcast(List<RpcEndpoint> endpoints, Class<S> serviceInterface, Function<S, T> endpointCallback) {
    // todo invoke in parallel so timeouts don't stack
    final Map<RpcEndpoint, RpcResult<T>> results = new HashMap<>();
    for (RpcEndpoint endpoint : endpoints) {
      try {
        results.put(endpoint, RpcResult.newSuccess(endpointCallback.apply(endpoint.service(serviceInterface))));
      } catch (Exception e) {
        results.put(endpoint, RpcResult.newFailure(e));
        LOGGER.error("Failed to apply callback for endpoint {}", RedactableArgument.system(endpoint), e);
      }
    }
    return results;
  }

  private static class RpcResult<T> {
    private T result;
    private Throwable failure;

    public static <T> RpcResult<T> newSuccess(T value) {
      return new RpcResult<>(value, null);
    }

    public static <Void> RpcResult<Void> newSuccess() {
      return new RpcResult<>(null, null);
    }

    public static <T> RpcResult<T> newFailure(Throwable t) {
      return new RpcResult<>(null, t);
    }

    private RpcResult(T result, Throwable failure) {
      this.result = result;
      this.failure = failure;
    }

    public T get() {
      if (failure != null) {
        Throwables.throwIfUnchecked(failure);
        throw new RuntimeException(failure);
      }
      return result;
    }

    public boolean isFailed() {
      return failure != null;
    }

    @Override
    public String toString() {
      return "BroadcastResult{" +
          "result=" + result +
          ", failure=" + failure +
          '}';
    }
  }

  private <S, T> Map<RpcEndpoint, RpcResult<Void>> broadcastVoid(Class<S> serviceInterface, Consumer<S> endpointCallback) {
    final List<RpcEndpoint> endpoints = listRpcEndpoints(consul.keyValueClient(), serviceName, Duration.ofSeconds(15));
    return broadcastVoid(endpoints, serviceInterface, endpointCallback);
  }

  private <S, T> Map<RpcEndpoint, RpcResult<Void>> broadcastVoid(List<RpcEndpoint> endpoints, Class<S> serviceInterface, Consumer<S> endpointCallback) {
    // todo invoke in parallel so timeouts don't stack
    final Map<RpcEndpoint, RpcResult<Void>> results = new HashMap<>();
    for (RpcEndpoint endpoint : endpoints) {
      try {
        endpointCallback.accept(endpoint.service(serviceInterface));
        results.put(endpoint, RpcResult.newSuccess());
      } catch (Exception e) {
        results.put(endpoint, RpcResult.newFailure(e));
        LOGGER.error("Failed to apply callback for endpoint {}", RedactableArgument.system(endpoint), e);
      }
    }
    return results;
  }

  public void stopStreaming() throws InterruptedException {
    while (true) {
      throwIfDone();

      final List<RpcEndpoint> endpoints = listRpcEndpoints(consul.keyValueClient(), serviceName, Duration.ofSeconds(15));
      final Map<RpcEndpoint, RpcResult<Void>> stopResults = broadcastVoid(endpoints, WorkerService.class, WorkerService::stopStreaming);

      if (stopResults.entrySet().stream()
          .noneMatch(e -> e.getValue().isFailed())) {
        return;
      }

      SECONDS.sleep(3);
    }
  }

  /**
   * Returns all ready endpoints. Blocks until at least one endpoint is ready.
   */
  public List<RpcEndpoint> awaitReadyEndpoints() throws InterruptedException {
    while (true) {
      throwIfDone();

      final List<RpcEndpoint> allEndpoints = listRpcEndpoints(consul.keyValueClient(), serviceName, Duration.ofSeconds(15));

      final List<RpcEndpoint> readyEndpoints = allEndpoints.stream()
          .filter(rpcEndpoint -> {
            try {
              rpcEndpoint.service(WorkerService.class).ready();
              return true;
            } catch (Throwable t) {
              LOGGER.warn("Endpoint {} is not ready; excluding it from rebalance.", rpcEndpoint, t);
              return false;
            }
          }).collect(Collectors.toList());

      if (!readyEndpoints.isEmpty()) {
        return readyEndpoints;
      }

      // todo truncated exponential backoff with a longer sleep time?
      SECONDS.sleep(5);
    }
  }

  public void rebalance() throws InterruptedException {
    LOGGER.info("Rebalancing the cluster");
    // dumb strategy: shut everything down, then reassign vbuckets
    stopStreaming();

    final List<RpcEndpoint> endpoints = awaitReadyEndpoints();
    final List<List<Integer>> vbucketChunks = chunks(allPartitions(numVbuckets), endpoints.size());

    for (int i = 0; i < endpoints.size(); i++) {
      throwIfDone();

      final Collection<Integer> vbuckets = vbucketChunks.get(i);
      final RpcEndpoint endpoint = endpoints.get(i);
      LOGGER.info("Assigning vbuckets to endpoint {} : {}", endpoint, vbuckets);
      try {
        endpoint.service(WorkerService.class).assignVbuckets(vbuckets);
      } catch (Throwable t) {
        // todo what happens here? What if it fails due to timeout, and the worker is actually doing the work?
        // for now, assume the leader will notice and fix the failure at some time in the future.
        LOGGER.warn("Failed to assign vbuckets to endpoint {}", endpoint, t);
      }
    }
  }

  public synchronized void stop() {
    done = true;
    if (thread != null) {
      thread.interrupt();
    }
    thread = null;
  }

  private void throwIfDone() throws InterruptedException {
    if (done) {
      throw new InterruptedException("Leader termination requested.");
    }
  }

  public static void main(String[] args) throws Exception {
    Consul consul = Consul.newClient();
    LeaderTask leader = new LeaderTask(consul, Sandbox.serviceName, 64);
    leader.rebalance();
  }
}
