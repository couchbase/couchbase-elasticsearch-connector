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

package com.couchbase.connector.cluster.consul.rpc;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;

public class Broadcaster implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(Broadcaster.class);

  private final ExecutorService executor = Executors.newCachedThreadPool();

  public <S> Map<RpcEndpoint, RpcResult<Void>> broadcast(String description, List<RpcEndpoint> endpoints, Class<S> serviceInterface, Consumer<S> endpointCallback) {
    return broadcast(description, endpoints, serviceInterface, s -> {
      endpointCallback.accept(s);
      return null;
    });
  }

  public <S, T> Map<RpcEndpoint, RpcResult<T>> broadcast(String description, List<RpcEndpoint> endpoints, Class<S> serviceInterface, Function<S, T> endpointCallback) {
    LOGGER.info("Broadcasting '{}' request to {} endpoints", description, endpoints.size());

    final Stopwatch timer = Stopwatch.createStarted();

    final List<Future<RpcResult<T>>> futures = new ArrayList<>();
    for (RpcEndpoint endpoint : endpoints) {
      futures.add(executor.submit(
          () -> RpcResult.newSuccess(endpointCallback.apply(endpoint.service(serviceInterface)))));
    }

    LOGGER.info("Scheduled all '{}' requests for broadcast. Awaiting responses...", description);

    final Map<RpcEndpoint, RpcResult<T>> results = new HashMap<>();
    for (int i = 0; i < endpoints.size(); i++) {
      final RpcEndpoint endpoint = endpoints.get(i);
      final Future<RpcResult<T>> f = futures.get(i);

      try {
        results.put(endpoint, f.get());

      } catch (Throwable e) {
        if (e instanceof ExecutionException) {
          e = e.getCause();
        }
        results.put(endpoint, RpcResult.newFailure(e));
        LOGGER.error("Failed to apply '{}' callback for endpoint {}", description, redactSystem(endpoint), e);
      }
    }

    LOGGER.info("Finished collecting '{}' broadcast responses. Broadcasting to {} endpoints took {}", description, endpoints.size(), timer);
    LOGGER.debug("Broadcast results: {}", results);
    return results;
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }
}
