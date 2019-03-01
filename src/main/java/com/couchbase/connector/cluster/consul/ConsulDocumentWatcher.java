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

import com.google.common.primitives.Longs;
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
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.couchbase.connector.cluster.consul.ReactorHelper.await;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ConsulDocumentWatcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsulDocumentWatcher.class);

  // Build a new Consul client for each watch so the watch can be cancelled by destroying the client.
  // See https://github.com/rickfast/consul-client/issues/307
  private final Consul.Builder consulBuilder;

  private final String pollingIntervalString;

  public ConsulDocumentWatcher(Consul.Builder consulBuilder) {
    this(consulBuilder, Duration.ofMinutes(5));
  }

  private ConsulDocumentWatcher(Consul.Builder consulBuilder, Duration pollingInterval) {
    this.consulBuilder = requireNonNull(consulBuilder);
    final long requestedPollingIntervalSeconds = MILLISECONDS.toSeconds(pollingInterval.toMillis());
    this.pollingIntervalString = Longs.constrainToRange(requestedPollingIntervalSeconds, 1, MINUTES.toSeconds(5)) + "s";
  }

  public ConsulDocumentWatcher withPollingInterval(Duration pollingInterval) {
    return new ConsulDocumentWatcher(this.consulBuilder, pollingInterval);
  }

  public Optional<String> awaitCondition(String key, Predicate<Optional<String>> valueCondition) throws InterruptedException {
    return await(watch(key), valueCondition);
  }

  public <T> Optional<T> awaitCondition(String key, Function<String, T> valueMapper, Predicate<Optional<T>> condition)
      throws InterruptedException {
    return await(watch(key).map(s -> s.map(valueMapper)), condition);
  }

  public <T> Optional<T> awaitCondition(String key, Function<String, T> valueMapper, Predicate<Optional<T>> condition, Duration timeout)
      throws InterruptedException, TimeoutException {
    return await(watch(key).map(s -> s.map(valueMapper)), condition, timeout);
  }

  /**
   * Blocks until the document does not exist or the current thread is interrupted.
   *
   * @param key the document to watch
   * @throws InterruptedException if the current thread is interrupted while waiting for document state to change.
   */
  public void awaitAbsence(String key) throws InterruptedException {
    awaitCondition(key, doc -> !doc.isPresent());
  }

  public Optional<String> awaitValueChange(String key, String valueBeforeChange) throws InterruptedException {
    return awaitCondition(key, doc -> !doc.equals(Optional.ofNullable(valueBeforeChange)));
  }

  public Flux<Optional<String>> watch(String key) {
    return watch(key, consulResponse -> consulResponse.getResponse()
        .map(documentBody -> documentBody.getValueAsString(UTF_8).orElse("")));
  }

  /**
   * Returns a flux whose values are published on one of the async I/O threads owned by the Consul library.
   */
  private <T> Flux<T> watch(String key, Function<ConsulResponse<Optional<Value>>, T> resultExtractor) {
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

  private <T> void watchOnce(FluxSink<T> emitter,
                             Consul consul,
                             String key,
                             Function<ConsulResponse<Optional<Value>>, T> resultExtractor,
                             BigInteger index,
                             AtomicBoolean done,
                             Object cancellationLock) {
    final QueryOptions options = ImmutableQueryOptions.builder()
        .index(index)
        .wait(pollingIntervalString)
        .build();

    LOGGER.debug("Watching for changes to {} with options {}", key, options);

    consul.keyValueClient().getValue(key, options,
        new ConsulResponseCallback<Optional<Value>>() {
          @Override
          public void onComplete(ConsulResponse<Optional<Value>> consulResponse) {
            try {
              final boolean waitTimedOut = consulResponse.getIndex().equals(index);
              if (!waitTimedOut) {
                emitter.next(resultExtractor.apply(consulResponse));
              }

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
