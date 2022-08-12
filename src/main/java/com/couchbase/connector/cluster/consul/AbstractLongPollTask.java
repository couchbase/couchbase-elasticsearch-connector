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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.Uninterruptibles.joinUninterruptibly;
import static java.util.Objects.requireNonNull;

public abstract class AbstractLongPollTask<SELF extends AbstractLongPollTask> implements AutoCloseable {
  private static final AtomicInteger threadCounter = new AtomicInteger();
  private final Logger LOGGER = LoggerFactory.getLogger(getClass());
  private final Thread thread;
  private volatile boolean closed;

  public AbstractLongPollTask(ConsulContext ctx, String threadNamePrefix, String sessionId) {
    requireNonNull(ctx);
    requireNonNull(sessionId);
    this.thread = new Thread(() -> doRun(ctx, sessionId), threadNamePrefix + threadCounter.getAndIncrement());
  }

  protected abstract void doRun(ConsulContext ctx, String sessionId);

  public void awaitTermination() {
    if (!closed) {
      throw new IllegalStateException("must call close() first");
    }

    Duration timeout = Duration.ofMinutes(1);
    LOGGER.info("Waiting for thread {} to terminate.", thread);
    joinUninterruptibly(thread, timeout);
    if (thread.isAlive()) {
      LOGGER.error("Thread {} did not terminate within {}.", thread, timeout);
    } else {
      LOGGER.info("Thread {} terminated.", thread.getName());
    }
  }

  /**
   * @throws IllegalStateException if already started
   */
  @SuppressWarnings("unchecked")
  public SELF start() {
    try {
      thread.start();
      LOGGER.info("Thread {} started.", thread.getName());
      return (SELF) this;

    } catch (IllegalThreadStateException e) {
      throw new IllegalStateException("May only be started once", e);
    }
  }

  protected boolean closed() {
    return closed;
  }

  public void close(boolean awaitTermination) {
    closed = true;

    LOGGER.info("Asking thread {} to terminate.", thread.getName());
    thread.interrupt();

    if (awaitTermination) {
      awaitTermination();
    }
  }

  @Override
  public void close() {
    close(true);
  }

}
