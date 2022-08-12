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

import com.couchbase.connector.elasticsearch.io.BackoffPolicy;
import com.couchbase.consul.ConsulHttpClient;
import com.couchbase.consul.ConsulOps;
import com.couchbase.consul.ConsulResponse;
import com.couchbase.consul.KvReadResult;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static com.couchbase.connector.cluster.consul.ConsulContext.formatEndpointId;
import static com.couchbase.connector.cluster.consul.ReactorHelper.blockSingle;
import static com.couchbase.connector.cluster.consul.ReactorHelper.logOnChange;
import static com.couchbase.connector.elasticsearch.io.BackoffPolicyBuilder.truncatedExponentialBackoff;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ConsulHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsulHelper.class);

  private ConsulHelper() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Returns {@code true} if the given session owned the lock, otherwise {@code false}.
   */
  public static boolean unlockAndDelete(ConsulHttpClient httpClient, String key, String sessionId) {
    List<Map<String, Object>> transaction = List.of(
        Map.of("KV", Map.of(
                "Verb", "unlock",
                "Key", key,
                "Session", sessionId
            )
        ),
        Map.of("KV", Map.of(
                "Verb", "delete",
                "Key", key
            )
        )
    );

    ConsulResponse<ObjectNode> response = blockSingle(
        httpClient.put("/txn")
            .bodyJson(transaction)
            .buildWithResponseType(ObjectNode.class)
    );

    if (response.httpStatusCode() == HTTP_CONFLICT) {
      return false;
    }

    response.requireSuccess();
    return true;
  }

  private static final String missingDocumentValue = "";

  public static long requireModifyIndex(ConsulResponse<Optional<KvReadResult>> response) {
    return response.body().map(KvReadResult::modifyIndex)
        .orElseThrow(() -> new RuntimeException("KV read response missing index"));
  }

  public static void atomicUpdate(ConsulOps.KvOps kv, String key, Function<String, String> mutator) throws IOException {
    final Duration timeout = Duration.ofSeconds(15);
    BackoffPolicy backoffPolicy =
        truncatedExponentialBackoff(Duration.ofMillis(10), Duration.ofSeconds(1))
            .fullJitter()
            .timeout(timeout)
            .build();

    final Iterator<Duration> waitIntervals = backoffPolicy.iterator();

    int attempt = 1;

    while (true) {
      final ConsulResponse<Optional<KvReadResult>> r = blockSingle(kv.readOneKey(key));
      if (r.body().isEmpty()) {
        // Don't automatically create the document, because it might need to be associated with another node's session.
        // For example, an RPC endpoint doc is updated by both client and server, but is tied to the server session.
        throw new IOException("Can't update non-existent document: " + key);
      }

      final long index = requireModifyIndex(r);
      final String oldValue = r.body().get().valueAsString();
      final String newValue = mutator.apply(oldValue);

      if (Objects.equals(newValue, oldValue)) {
        LOGGER.debug("Atomic update of key {} would be a no-op; skipping!", key);
        return;
      }

      boolean success = blockSingle(kv.upsertKey(key, newValue, Map.of("cas", index))).body();
      if (success) {
        LOGGER.debug("Atomic update of key {} completed successfully", key);
        return;
      }

      if (!waitIntervals.hasNext()) {
        throw new RuntimeException("Atomic update of key '" + key + "' could not complete after " + attempt + " attempts within " + Duration.ofMinutes(1));
      }
      final Duration retryDelay = waitIntervals.next();
      attempt++;
      LOGGER.debug("Retrying atomic update of key {} in {} (attempt {})", key, retryDelay, attempt);
      try {
        MILLISECONDS.sleep(retryDelay.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }

  public static ConsulResponse<Optional<KvReadResult>> getWithRetry(ConsulOps.KvOps kv, String key, BackoffPolicy backoffPolicy) throws TimeoutException {
    final Iterator<Duration> retryDelays = backoffPolicy.iterator();

    while (true) {
      final ConsulResponse<Optional<KvReadResult>> response = blockSingle(kv.readOneKey(key));
      if (response.body().isPresent()) {
        return response;
      }

      try {
        if (!retryDelays.hasNext()) {
          break;
        }

        final Duration retryDelay = retryDelays.next();
        LOGGER.debug("Document does not exist; sleeping for {} and then trying again to get {}", retryDelay, key);
        MILLISECONDS.sleep(retryDelay.toMillis());

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    throw new TimeoutException("getWithRetry timed out for key " + key);
  }


  public static void atomicUpdate(ConsulOps.KvOps kv, String key, ConsulResponse<Optional<KvReadResult>> initialResponse, Function<String, String> mutator) throws IOException {
    LOGGER.debug("Updating key {}", key);

    final String oldValue = initialResponse.body()
        .map(KvReadResult::valueAsString)
        .orElse(missingDocumentValue);
    final String newValue = mutator.apply(oldValue);

    if (Objects.equals(newValue, oldValue)) {
      return;
    }

    final long index = requireModifyIndex(initialResponse);
    boolean success = blockSingle(kv.upsertKey(key, newValue, Map.of("cas", index))).body();

    if (!success) {
      LOGGER.debug("Failed to put new document (optimistic locking failure?); reloading and retrying");
      atomicUpdate(kv, key, mutator);
    }
  }

  public static ConsulResponse<Optional<KvReadResult>> awaitChange(ConsulOps.KvOps kv, String key, long index) {
    while (true) {
      final ConsulResponse<Optional<KvReadResult>> response = blockSingle(
          kv.readOneKey(key, Map.of(
              "index", index,
              "wait", "5m"
          )));

      if (response.body().isEmpty()) {
        LOGGER.debug("Document does not exist: {}", key);
        return response;
      }
      long responseIndex = requireModifyIndex(response);
      if (responseIndex == index) {
        LOGGER.debug("Long poll timed out, polling again for {}", key);
      } else {
        return response;
      }
    }
  }

  public static Flux<ImmutableSet<String>> watchServiceHealth(ConsulOps consul, String serviceName) {
    Flux<ImmutableSet<String>> flux = new ConsulResourceWatcher()
        .watch(opts -> {
          Map<String, Object> options = new HashMap<>(opts);
          options.put("passing", true);
          return consul.health().health(serviceName, options);
        })
        .map(it -> it.requireSuccess().map(arrayNode ->
            ImmutableSet.copyOf(Iterables.transform(arrayNode, serviceNode ->
                formatEndpointId(
                    requireNonNull(serviceNode.path("Node").path("Node").textValue(), "missing Node.Node"),
                    requireNonNull(serviceNode.path("Node").path("Address").textValue(), "missing Node.Address"),
                    requireNonNull(serviceNode.path("Service").path("ID").asText(), "missing Service.ID")
                )))))
        .map(ConsulResponse::body)
        .distinctUntilChanged()
        .doFinally(signal ->
            LOGGER.info("Stopping health watch for service {}; reason: {}", serviceName, signal)
        );

    return logOnChange(flux, "Service health", LOGGER);
  }

  public static Flux<ImmutableSet<String>> watchServiceHealth(ConsulOps consul, String serviceName, Duration quietPeriod) {
    return watchServiceHealth(consul, serviceName)
        .doOnNext(set -> LOGGER.info("Waiting {} for service health to stabilize before rebalancing.", quietPeriod))
        .sampleTimeout(s -> Mono.delay(quietPeriod))
        .distinctUntilChanged()
        .doOnNext(set -> LOGGER.info("Service health stabilized; no changes in last {}.", quietPeriod));
  }
}
