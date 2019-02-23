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

import com.orbitz.consul.Consul;
import com.orbitz.consul.async.ConsulResponseCallback;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.kv.Value;
import com.orbitz.consul.option.ImmutableQueryOptions;
import com.orbitz.consul.option.QueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.math.BigInteger;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Reactive utilities for Consul.
 */
public class ConsulReactor {
  private ConsulReactor() {
    throw new AssertionError("not instantiable");
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsulReactor.class);

  public static Flux<Optional<String>> watch(Consul.Builder consulBuilder, String key) {
    return watch(consulBuilder, key, consulResponse -> consulResponse.getResponse()
        .map(documentBody -> documentBody.getValueAsString(UTF_8).orElse("")));
  }

  /**
   * Returns a flux whose values are published on one of the async I/O threads owned by the Consul library.
   */
  public static <T> Flux<T> watch(Consul.Builder consulBuilder, String key, Function<ConsulResponse<Optional<Value>>, T> resultExtractor) {
    requireNonNull(consulBuilder);
    requireNonNull(key);
    requireNonNull(resultExtractor);

    return Flux.create(emitter -> {
      // Build a new instance we can safely terminate to cancel outstanding requests when flux is disposed.
      final Consul consul = consulBuilder.build();

      final AtomicBoolean done = new AtomicBoolean();
      final Object cancellationLock = new Object();

      emitter.onDispose(() -> {
        synchronized (cancellationLock) {
          LOGGER.debug("Cancelling watch for key {}", key);
          done.set(true); // advisory flag to prevent starting a new request
          consul.destroy(); // terminate an in-flight request (but doesn't prevent new ones, oddly!)
        }
      });

      try {
        watchOnce(emitter, consul, key, resultExtractor, BigInteger.ZERO, done, cancellationLock);
      } catch (Throwable t) {
        emitter.error(t);
      }

    }, FluxSink.OverflowStrategy.LATEST);
  }

  private static <T> void watchOnce(FluxSink<T> emitter,
                                    Consul consul,
                                    String key,
                                    Function<ConsulResponse<Optional<Value>>, T> resultExtractor,
                                    BigInteger index,
                                    AtomicBoolean done,
                                    Object cancellationLock) {

    LOGGER.debug("Watching for changes to {} with index {}", key, index);

    final QueryOptions options = ImmutableQueryOptions.builder()
        .index(index)
        .wait("5m")
        .build();

    consul.keyValueClient().getValue(key, options,
        new ConsulResponseCallback<Optional<Value>>() {
          @Override
          public void onComplete(ConsulResponse<Optional<Value>> consulResponse) {
            try {
              emitter.next(resultExtractor.apply(consulResponse));

              synchronized (cancellationLock) {
                if (done.get()) {
                  // Flux was cancelled in the brief time when there's no in-flight Consul request.
                  return;
                }

                watchOnce(emitter, consul, key, resultExtractor, consulResponse.getIndex(), done, cancellationLock);
              }

            } catch (Throwable t) {
              LOGGER.error("Failure in key watcher callback onComplete", t);
              handleError(t);
            }
          }

          @Override
          public void onFailure(Throwable t) {
            if (!done.get()) {
              handleError(t);
            }
          }

          private void handleError(Throwable t) {
            try {
              emitter.error(t);
            } finally {
              consul.destroy();
            }
          }
        }
    );
  }
}
