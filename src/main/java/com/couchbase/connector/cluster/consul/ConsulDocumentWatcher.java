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

import com.couchbase.consul.ConsulOps;
import com.couchbase.consul.KvReadResult;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.couchbase.connector.cluster.consul.ReactorHelper.await;
import static java.util.Objects.requireNonNull;

public class ConsulDocumentWatcher {
  private final ConsulOps consul;
  private final ConsulResourceWatcher resourceWatcher;

  public ConsulDocumentWatcher(ConsulOps consul) {
    this(consul, Duration.ofMinutes(5));
  }

  private ConsulDocumentWatcher(ConsulOps consul, Duration pollingInterval) {
    this.consul = requireNonNull(consul);
    this.resourceWatcher = new ConsulResourceWatcher().withPollingInterval(pollingInterval);
  }

  public ConsulDocumentWatcher withPollingInterval(Duration pollingInterval) {
    return new ConsulDocumentWatcher(this.consul, pollingInterval);
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
    awaitCondition(key, Optional::isEmpty);
  }

  public Optional<String> awaitValueChange(String key, String valueBeforeChange) throws InterruptedException {
    return awaitCondition(key, doc -> !doc.equals(Optional.ofNullable(valueBeforeChange)));
  }

  public Flux<Optional<String>> watch(String key) {
    requireNonNull(key);
    return resourceWatcher.watch(opts -> consul.kv().readOneKey(key, opts))
        .map(it -> it.body().map(KvReadResult::valueAsString))
        .distinctUntilChanged();
  }
}
