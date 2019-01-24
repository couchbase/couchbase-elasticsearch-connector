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
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.kv.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.math.BigInteger;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Reactive utilities for Consul.
 */
public class ConsulReactor {
  private ConsulReactor() {
    throw new AssertionError("not instantiable");
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsulReactor.class);

  private static final AtomicLong nextId = new AtomicLong();

  public static Flux<String> watch(Consul.Builder builder, String key, Duration existencePollingInterval) {
    return watchWhileExists(builder, key, valueAsString())
        .repeat(afterDelay(existencePollingInterval));
  }

  private static Function<ConsulResponse<Value>, String> valueAsString() {
    return value -> value.getResponse().getValueAsString(UTF_8).orElse("");
  }

  private static BooleanSupplier afterDelay(Duration delay) {
    requireNonNull(delay);

    return () -> {
      try {
        MILLISECONDS.sleep(delay.toMillis() + 1);
        return true;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    };
  }

  private static <T> Flux<T> watchWhileExists(Consul.Builder consulBuilder, String key, Function<ConsulResponse<Value>, T> resultExtractor) {
    return Flux.create(emitter -> {
      // Build a new instance we can safely terminate to cancel outstading requests when flux is disposed.
      final Consul consul = consulBuilder.build();

      final AtomicBoolean done = new AtomicBoolean();
      final Thread worker = new Thread(() -> {
        LOGGER.debug("Document watcher thread started: {}", Thread.currentThread());

        try {
          BigInteger index = BigInteger.ZERO;

          while (!done.get()) {
            LOGGER.debug("Watching for changes to {} with index {}", key, index);
            final ConsulResponse<Value> value = ConsulHelper.awaitChange(consul.keyValueClient(), key, index);
            if (value == null) {
              LOGGER.debug("Document {} does not exist.", key);
              emitter.complete();
              return;
            }
            LOGGER.debug("Emitting new value for document {}", key);
            emitter.next(resultExtractor.apply(value));
            index = value.getIndex();
          }

        } catch (Throwable t) {
          LOGGER.debug("Exception in document watcher loop (this is expected if flux is cancelled).", t);
          if (!done.get()) {
            emitter.error(t);
          }
        } finally {
          consul.destroy();
          LOGGER.debug("Thread exiting: {}", Thread.currentThread());
        }
      });

      emitter.onDispose(() -> {
        LOGGER.debug("Asking thread {} to stop.", worker);
        done.set(true); // prevent looping
        worker.interrupt(); // prevent a new request from starting if already in loop
        consul.destroy(); // terminate an in-flight request
      });

      worker.setName("consul-key-watcher-" + nextId.getAndIncrement() + "(" + key + ")");
      worker.setDaemon(true);
      worker.start();

    }, FluxSink.OverflowStrategy.LATEST);
  }
}
