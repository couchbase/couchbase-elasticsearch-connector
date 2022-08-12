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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.Uninterruptibles.awaitTerminationUninterruptibly;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * Registers the service with Consul, establishes a session, and handles heartbeats.
 */
public class SessionTask implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionTask.class);

  private final ConsulContext ctx;
  private final String sessionId;
  private final Runnable runWhenHealthCheckPassed;
  private final Consumer<Throwable> fatalErrorConsumer;
  private volatile boolean shouldPassHealthCheck = true;
  private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
  private volatile boolean started = false;

  private static final Duration HEALTH_CHECK_INTERVAL = Duration.ofSeconds(15);

  public SessionTask(ConsulContext consulContext, Runnable runWhenHealthCheckPassed, Consumer<Throwable> fatalErrorConsumer) {
    this.ctx = requireNonNull(consulContext);
    this.runWhenHealthCheckPassed = requireNonNull(runWhenHealthCheckPassed);
    this.fatalErrorConsumer = requireNonNull(fatalErrorConsumer);

    try {
      // todo catch exception, retry with backoff (wait for consul agent to start)
      final Duration ttl = HEALTH_CHECK_INTERVAL.multipliedBy(2);
      ctx.register(ttl);

      passHealthCheck();

      this.sessionId = ctx.createSession();
      LOGGER.info("Registered service {} and started session {}", ctx.serviceId(), sessionId);

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

    final int delay = Math.toIntExact(HEALTH_CHECK_INTERVAL.toMillis());
    executorService.scheduleWithFixedDelay(this::passHealthCheck, delay, delay, MILLISECONDS);

    return this;
  }

  public synchronized void close() throws Exception {
    LOGGER.info("Stopping refresh task for session {}", sessionId);

    shouldPassHealthCheck = false;
    executorService.shutdownNow();

    final Duration shutdownTimeout = Duration.ofSeconds(30);
    if (!awaitTerminationUninterruptibly(executorService, shutdownTimeout)) {
      LOGGER.warn("Consul health check executor for session {} failed to shut down within {}.", sessionId, shutdownTimeout);
    }
  }

  public synchronized void passHealthCheck() {
    if (!shouldPassHealthCheck) {
      // throwing an exception stops the scheduled task from running again.
      throw new RuntimeException("Stopping recurring health check task.");
    }

    try {
      ctx.passHealthCheck();

      try {
        runWhenHealthCheckPassed.run();
      } catch (Throwable t) {
        LOGGER.error("Callback failed for passed health check", t);
      }

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
