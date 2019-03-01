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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.orbitz.consul.Consul;
import com.orbitz.consul.ConsulException;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.cache.ServiceHealthCache;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.agent.Member;
import com.orbitz.consul.model.health.ServiceHealth;
import com.orbitz.consul.model.kv.ImmutableOperation;
import com.orbitz.consul.model.kv.Value;
import com.orbitz.consul.model.kv.Verb;
import com.orbitz.consul.option.ImmutablePutOptions;
import com.orbitz.consul.option.ImmutableQueryOptions;
import com.orbitz.consul.option.PutOptions;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ConsulHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsulHelper.class);

  private ConsulHelper() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Returns {@code true} if the given session owned the lock, otherwise {@code false}.
   */
  public static boolean unlockAndDelete(KeyValueClient kv, String key, String sessionId) {
    try {
      kv.performTransaction(
          ImmutableOperation.builder(Verb.UNLOCK).key(key).session(sessionId).build(),
          ImmutableOperation.builder(Verb.DELETE).key(key).build());
      return true;

    } catch (ConsulException e) {
      if (e.getCode() == HTTP_CONFLICT) {
        return false; // didn't own lock; no worries
      }
      throw e;
    }
  }

  private static final String missingDocumentValue = "";

  private static void atomicUpdate(KeyValueClient kv, String key, Function<String, String> mutator) throws IOException {
    while (true) {
      final ConsulResponse<Value> r = kv.getConsulResponseWithValue(key).orElse(null);
      if (r == null) {
        // Don't automatically create the document, because it might need to be associated with another node's session.
        // For example, an RPC endpoint doc is updated by both client and server, but is tied to the server session.
        throw new IOException("Can't update non-existent document: " + key);
      }

      final BigInteger index = r.getIndex();
      final String oldValue = r.getResponse().getValueAsString(UTF_8).orElse(missingDocumentValue);
      final String newValue = mutator.apply(oldValue);

      if (Objects.equals(newValue, oldValue)) {
        return;
      }

      final PutOptions options = ImmutablePutOptions.builder().cas(index.longValue()).build();
      boolean success = kv.putValue(key, newValue, 0, options, UTF_8);
      if (success) {
        return;
      }

      // todo truncated exponential backoff, please! Die if timeout!
      //MILLISECONDS.sleep(100);
    }
  }

  public static ConsulResponse<Value> getWithRetry(KeyValueClient kv, String key, BackoffPolicy backoffPolicy) throws TimeoutException {
    final Iterator<TimeValue> retryDelays = backoffPolicy.iterator();

    while (true) {
      final ConsulResponse<Value> response = kv.getConsulResponseWithValue(key).orElse(null);
      if (response != null) {
        return response;
      }

      try {
        if (!retryDelays.hasNext()) {
          break;
        }

        final TimeValue retryDelay = retryDelays.next();
        LOGGER.debug("Document does not exist; sleeping for {} and then trying again to get {}", retryDelay, key);
        retryDelay.timeUnit().sleep(retryDelay.duration());

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    throw new TimeoutException("getWithRetry timed out for key " + key);
  }

  public static void atomicUpdate(KeyValueClient kv, ConsulResponse<Value> initialResponse, Function<String, String> mutator) throws IOException {
    final Value v = initialResponse.getResponse();
    final String key = v.getKey();

    LOGGER.debug("Updating key {}", key);

    final String oldValue = v.getValueAsString(UTF_8).orElse(missingDocumentValue);
    final String newValue = mutator.apply(oldValue);

    if (Objects.equals(newValue, oldValue)) {
      return;
    }

    final long index = initialResponse.getIndex().longValue();
    final PutOptions options = ImmutablePutOptions.builder().cas(index).build();
    boolean success = kv.putValue(key, newValue, 0, options, UTF_8);

    if (!success) {
      LOGGER.debug("Failed to put new document (optimistic locking failure?); reloading and retrying");
      atomicUpdate(kv, key, mutator);
    }
  }

  public static ConsulResponse<Value> awaitChange(KeyValueClient kv, String key, BigInteger index) {
    while (true) {
      final ConsulResponse<Value> response = kv.getConsulResponseWithValue(key,
          ImmutableQueryOptions.builder()
              .index(index)
              .wait("5m")
              .build())
          .orElse(null);
      if (response == null) {
        LOGGER.debug("Document does not exist: {}", key);
        return null;
      }

      if (index.equals(response.getIndex())) {
        LOGGER.debug("Long poll timed out, polling again for {}", key);
      } else {
        return response;
      }
    }
  }

  public static List<String> listKeys(KeyValueClient kv, String keyPrefix) {
    try {
      return kv.getKeys(keyPrefix);

    } catch (ConsulException e) {
      if (e.getCode() == HTTP_NOT_FOUND) {
        return new ArrayList<>(0);
      }
      throw e;
    }
  }

  public static Flux<ImmutableSet<String>> watchServiceHealth(Consul.Builder consulBuilder, String serviceName) {
    final Flux<ImmutableSet<String>> flux = Flux.create(emitter -> {
      // Build a new instance we can safely terminate to cancel outstanding requests when flux is disposed.
      final Consul consul = consulBuilder.build();
      final ServiceHealthCache svHealth = ServiceHealthCache.newCache(consul.healthClient(), serviceName);

      emitter.onDispose(() -> {
        try {
          LOGGER.debug("Cancelling health watch for service {}", serviceName);
          svHealth.stop();
        } finally {
          consul.destroy();
        }
      });

      try {
        svHealth.addListener(newValues -> emitter.next(
            ImmutableSet.copyOf(
                newValues.values().stream()
                    .map(ConsulHelper::endpointId)
                    .collect(Collectors.toSet()))));

        svHealth.start();

      } catch (Throwable t) {
        emitter.error(t);
      }
    }, FluxSink.OverflowStrategy.LATEST);

    final AtomicReference<ImmutableSet<String>> prev = new AtomicReference<>(ImmutableSet.of());

    return flux
        .distinctUntilChanged()
        .doOnNext(currentEndpointIds -> {
          if (LOGGER.isInfoEnabled()) {
            final Set<String> joiningNodes = Sets.difference(currentEndpointIds, prev.get());
            final Set<String> leavingNodes = Sets.difference(prev.get(), currentEndpointIds);
            prev.set(currentEndpointIds);
            LOGGER.info("Service health changed; Joining: {} Leaving: {}", joiningNodes, leavingNodes);
          }
        });
  }

  public static Flux<ImmutableSet<String>> watchServiceHealth(Consul.Builder consulBuilder, String serviceName, Duration quietPeriod) {
    return watchServiceHealth(consulBuilder, serviceName)
        .doOnNext(set -> LOGGER.info("Waiting for service health to stabilize before rebalancing, quiet period = {}", quietPeriod))
        .sampleTimeout(s -> Mono.delay(quietPeriod))
        .distinctUntilChanged();
  }

  public static String endpointId(Member member, String serviceId) {
    return member.getName() + "::" + member.getAddress() + "::" + serviceId;
  }

  public static String endpointId(ServiceHealth health) {
    return health.getNode().getNode() + "::" + health.getNode().getAddress() + "::" + health.getService().getId();
  }
}
