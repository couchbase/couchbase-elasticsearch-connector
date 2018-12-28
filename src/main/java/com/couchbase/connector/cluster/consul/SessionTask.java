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

import com.google.common.base.Throwables;
import com.orbitz.consul.Consul;
import com.orbitz.consul.model.session.ImmutableSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;


/**
 * Registers the service with Consul, establishes a session, and handles heartbeats.
 */
public class SessionTask implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionTask.class);

  private final Consul consul;
  private final String serviceName;
  private final String serviceId;
  private final String serviceUuid = UUID.randomUUID().toString();
  private final String sessionId;
  private final Consumer<Throwable> fatalErrorConsumer;
  private volatile boolean shouldPassHealthCheck = true;
  private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
  private volatile boolean started = false;

  private static final int HEALTH_CHECK_INTERVAL_SECONDS = 15;

  public SessionTask(Consul consul, String serviceName, String serviceId, Consumer<Throwable> fatalErrorConsumer) {
    serviceId = defaultIfNull(serviceId, serviceName);
    this.serviceId = serviceId;
    this.serviceName = requireNonNull(serviceName);
    this.consul = requireNonNull(consul);
    this.fatalErrorConsumer = requireNonNull(fatalErrorConsumer);

    try {
      final List<String> tags = singletonList("couchbase-elasticsearch-connector");// emptyList();
      final Map<String, String> meta = singletonMap("uuid", serviceUuid);

      // todo catch exception, retry with backoff (wait for consul agent to start)
      final int sessionTtlSeconds = HEALTH_CHECK_INTERVAL_SECONDS * 2;
      consul.agentClient().register(0, sessionTtlSeconds, this.serviceName, this.serviceId, tags, meta);

      passHealthCheck();

      this.sessionId = consul.sessionClient().createSession(ImmutableSession.builder()
          .name("couchbase:cbes:" + this.serviceId)
          .behavior("delete")
          .lockDelay("15s")
          .addChecks("service:" + this.serviceId) // consul client library names the health check "service:<serviceId>"
          .build()
      ).getId();

    } catch (Throwable t) {
      fatalErrorConsumer.accept(t); // todo need to send to fatalErrorConsumer?
      throw t;
    }
  }

  public SessionTask start() {
    if (started) {
      throw new IllegalStateException("already started");
    }
    started = true;

    final int delay = HEALTH_CHECK_INTERVAL_SECONDS;
    executorService.scheduleWithFixedDelay(this::passHealthCheck, delay, delay, SECONDS);

    return this;
  }

  public synchronized void close() throws Exception {
    shouldPassHealthCheck = false;
    executorService.shutdown();

    final int shutdownTimeoutSeconds = 10;
    if (!executorService.awaitTermination(shutdownTimeoutSeconds, SECONDS)) {
      LOGGER.warn("Consul health check executor failed to shut down within {} seconds.", shutdownTimeoutSeconds);
    }

    // todo think some more about whether it would be good to de-register the service,
    // or if it's better for the service to remain in the Consul UI in "critical" status.
  }

  public synchronized void passHealthCheck() {
    if (!shouldPassHealthCheck) {
      // throwing an exception stops the scheduled task from running again.
      throw new RuntimeException("Stopping recurring health check task.");
    }

    try {
      consul.agentClient().pass(serviceId, "(" + serviceId + ") OK");
      LOGGER.debug("Passed health check.");

    } catch (Throwable t) {
      fatalErrorConsumer.accept(new RuntimeException("Failed to tell Consul agent we passed health check.", t));

      Throwables.throwIfUnchecked(t);
      throw new RuntimeException(t);
    }
  }

  public String sessionId() {
    return sessionId;
  }
}
