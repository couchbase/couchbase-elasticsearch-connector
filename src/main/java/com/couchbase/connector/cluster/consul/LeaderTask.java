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
import com.google.common.base.Throwables;
import com.orbitz.consul.Consul;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.couchbase.connector.cluster.consul.ConsulHelper.listRpcEndpoints;
import static com.couchbase.connector.dcp.DcpHelper.allPartitions;
import static com.couchbase.connector.util.ListHelper.chunks;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LeaderTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeaderTask.class);

  // Wait this long before assuming an unreachable worker node has stopped streaming.
  private static final Duration quietPeriodAfterFailedShutdownRequest = Duration.ofSeconds(30);

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

  public void stop() {
    done = true;
    if (thread != null) {
      thread.interrupt();
    }
    thread = null;
  }

  private void doRun() {
    LOGGER.info("Leader thread started.");

    try (NodeWatcher watcher = new NodeWatcher(consul, serviceName, Duration.ofSeconds(5))) {
      while (true) {
        throwIfDone();
        watcher.waitForNodesToJoinOrLeave();
        rebalance();
      }
    } catch (InterruptedException e) {
      // this is how the thread normally terminates.
      LOGGER.debug("Leader thread interrupted", e);
    } finally {
      LOGGER.info("Leader thread terminated.");
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

  private void stopStreaming() throws InterruptedException {
    int attempt = 1;

    // Repeat until all endpoints successfully acknowledge they have been shut down
    while (true) {
      throwIfDone();

      final List<RpcEndpoint> endpoints = listRpcEndpoints(consul.keyValueClient(), serviceName, Duration.ofSeconds(15));
      final Map<RpcEndpoint, RpcResult<Void>> stopResults = broadcastVoid(endpoints, WorkerService.class, WorkerService::stopStreaming);

      if (stopResults.entrySet().stream()
          .noneMatch(e -> e.getValue().isFailed())) {
        if (attempt != 1) {
          LOGGER.warn("Multiple attempts were required to quiesce the cluster. Sleeping for an additional {} to allow unreachable nodes to terminate.", quietPeriodAfterFailedShutdownRequest);
          sleep(quietPeriodAfterFailedShutdownRequest);
        }

        LOGGER.info("Cluster quiesced.");
        return;
      }

      LOGGER.warn("Attempt #{} to quiesce the cluster failed. Will retry.", attempt);

      attempt++;
      SECONDS.sleep(5);
    }
  }

  private static void sleep(Duration d) throws InterruptedException {
    MILLISECONDS.sleep(d.toMillis() + 1);
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

  private void rebalance() throws InterruptedException {
    restartRebalance:
    while (true) {
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
          // For now, start the whole rebalance process over again. This is obviously not idea.
          LOGGER.warn("Failed to assign vbuckets to endpoint {}", endpoint, t);
          continue restartRebalance;
        }
      }

      // success!
      return;
    }
  }

  private void throwIfDone() throws InterruptedException {
    if (done) {
      throw new InterruptedException("Leader termination requested.");
    }
  }
}
