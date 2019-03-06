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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import com.orbitz.consul.Consul;
import reactor.core.publisher.Flux;

import java.io.Closeable;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class ConsulContext implements Closeable {
  private final Consul primaryClient;
  private final Consul.Builder clientBuilder;
  private final DocumentKeys keys;
  private final String serviceName;
  private final String serviceId;
  private final ConsulDocumentWatcher documentWatcher;

  public ConsulContext(Consul.Builder clientBuilder, String serviceName, String serviceIdOrNull) {
    this.clientBuilder = requireNonNull(clientBuilder);
    this.primaryClient = clientBuilder.build();
    this.documentWatcher = new ConsulDocumentWatcher(clientBuilder);

    this.keys = new DocumentKeys(primaryClient.keyValueClient(), this.documentWatcher, serviceName);
    this.serviceName = requireNonNull(serviceName);
    this.serviceId = Optional.ofNullable(serviceIdOrNull).orElse(serviceName);
  }

  public Consul consul() {
    return primaryClient;
  }

  public Consul.Builder consulBuilder() {
    return clientBuilder;
  }

  public DocumentKeys keys() {
    return keys;
  }

  public String serviceName() {
    return serviceName;
  }

  public String serviceId() {
    return serviceId;
  }

  public Flux<Optional<String>> watchConfig() {
    return documentWatcher.watch(keys().config());
  }

  public Flux<Optional<String>> watchControl() {
    return documentWatcher.watch(keys().control());
  }

  public ConsulDocumentWatcher documentWatcher() {
    return documentWatcher;
  }

  public Flux<ImmutableSet<String>> watchServiceHealth(Duration quietPeriod) {
    return ConsulHelper.watchServiceHealth(clientBuilder, serviceName, quietPeriod);
  }

  @Override
  public void close() {
    primaryClient.destroy();
  }

  // Creates a temporary Consul client in an isolated thread and passes it to the given consumer.
  // Useful for cleanup code that should not be interrupted.
  public void runCleanup(Consumer<Consul> consumer) {
    final AtomicReference<Throwable> deferred = new AtomicReference<>();
    final Thread thread = new Thread(() -> {
      final Consul consul = consulBuilder().build();
      try {
        consumer.accept(consul);
      } catch (Throwable t) {
        deferred.set(t);
      } finally {
        consul.destroy();
      }
    });
    thread.start();

    final int timeoutSeconds = 30;
    Uninterruptibles.joinUninterruptibly(thread, timeoutSeconds, TimeUnit.SECONDS);
    if (thread.isAlive()) {
      throw new IllegalStateException("cleanup thread failed to complete within " + timeoutSeconds + " seconds.");
    }

    final Throwable t = deferred.get();
    if (t != null) {
      Throwables.throwIfUnchecked(t);
      throw new RuntimeException(t);
    }
  }
}
