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

import com.orbitz.consul.Consul;
import com.orbitz.consul.KeyValueClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

public abstract class AbstractLongPollTask<SELF extends AbstractLongPollTask> implements AutoCloseable {
  private static final AtomicInteger threadCounter = new AtomicInteger();
  private final Logger LOGGER = LoggerFactory.getLogger(getClass());
  private final Thread thread;
  private volatile boolean closed;

  public AbstractLongPollTask(KeyValueClient kv, String threadNamePrefix, DocumentKeys documentKeys, String sessionId) {
    requireNonNull(kv);
    requireNonNull(sessionId);
    requireNonNull(documentKeys);
    this.thread = new Thread(() -> doRun(kv, documentKeys, sessionId), threadNamePrefix + threadCounter.getAndIncrement());
  }

  protected abstract void doRun(KeyValueClient kv, DocumentKeys documentKeys, String sessionId);

  public void awaitTermination() throws InterruptedException {
    if (!closed) {
      throw new IllegalStateException("must call close() first");
    }
    LOGGER.info("Waiting for thread {} to terminate.", thread.getName());
    thread.join();
    LOGGER.info("Thread {} terminated.", thread.getName());
  }

  /**
   * Clears the <i>interrupted status</i> of the current thread.
   */
  protected static void clearInterrupted() {
    @SuppressWarnings("unused")
    boolean wasInterrupted = Thread.interrupted();
  }


  /**
   * @throws IllegalStateException if already started
   */
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

  /**
   * Requests a graceful shutdown. After calling this method, the caller should complete the shutdown process
   * by calling {@link Consul#destroy}, optionally followed by {@link #awaitTermination}.
   */
  @Override
  public void close() {
    closed = true;

    // Interrupting the thread prevents execution of new Consul request.
    // To cancel requests already in flight, the owner of the Consul client must destroy it.
    // This little dance is required because destroying the Consul client does not prevent
    // it from issuing new requests :-p
    thread.interrupt();

    // The thread will exit when it makes a new Consul request, the current in-flight request finishes,
    // or the Consul client is destroyed (whichever comes first).
    LOGGER.info("Asking thread {} to terminate.", thread.getName());
  }

}
