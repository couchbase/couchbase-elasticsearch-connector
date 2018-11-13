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
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LeaderElectionTask implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeaderElectionTask.class);

  private final AtomicInteger threadCounter = new AtomicInteger();
  private final Thread thread;
  private final String candidateUuid;// = UUID.randomUUID().toString();
  private final Consumer<Throwable> fatalErrorConsumer;
  private volatile boolean closed;

  public LeaderElectionTask(KeyValueClient kv, String serviceName, String sessionId, Consumer<Throwable> fatalErrorConsumer) {
    requireNonNull(kv);
    requireNonNull(sessionId);
    this.candidateUuid = sessionId;
    this.fatalErrorConsumer = requireNonNull(fatalErrorConsumer);
    this.thread = new Thread(() -> doRun(kv, serviceName, sessionId), "leader-election-" + threadCounter.getAndIncrement());
  }

  private void doRun(KeyValueClient kv, String serviceName, String sessionId) {
    final String leaderKey = "couchbase/cbes/" + serviceName + "/leader";
    LOGGER.info("Leader key: {}", leaderKey);

    try {
      while (!closed) {
        LOGGER.info("Racing to become new leader.");
        final boolean acquired = kv.acquireLock(leaderKey, candidateUuid, sessionId);

        if (acquired) {
          LOGGER.info("Won the leader election! {}", candidateUuid);

          String newLeader = ConsulHelper.awaitChange(kv, leaderKey, candidateUuid);
          LOGGER.info("No longer the leader; new leader is {}", newLeader);

        } else {
          LOGGER.info("Not the leader");

          // Sleeping because the attempt may have failed due to Consul's lock delay;
          // see https://www.consul.io/docs/internals/sessions.html
          SECONDS.sleep(1);

          ConsulHelper.awaitRemoval(kv, leaderKey);
        }
      }

    } catch (Throwable t) {
      if (closed) {
        // Closing the task is likely to result in an InterruptedException,
        // or a ConsulException wrapping an InterruptedIOException.
        LOGGER.debug("Caught exception in leader election loop after closing. Don't panic; this is expected.", t);
      } else {
        // Something went horribly, terribly, unrecoverably wrong.
        fatalErrorConsumer.accept(t);
      }

    } finally {
      try {
        // Abdicate (delete leadership document) if we own the lock.
        clearInterrupted(); // so final Consul request doesn't fail with InterruptedException
        ConsulHelper.unlockAndDelete(kv, leaderKey, sessionId);

      } catch (Exception e) {
        LOGGER.warn("Failed to abdicate", e);
      }

      LOGGER.info("Exiting leader election thread for session {}", sessionId);
    }
  }

  /**
   * Clears the <i>interrupted status</i> of the current thread.
   */
  private static void clearInterrupted() {
    @SuppressWarnings("unused")
    boolean wasInterrupted = Thread.interrupted();
  }

  /**
   * @throws IllegalStateException if already started
   */
  public LeaderElectionTask start() {
    try {
      thread.start();
      LOGGER.info("Leader election thread started.");
      return this;

    } catch (IllegalThreadStateException e) {
      throw new IllegalStateException("May only be started once", e);
    }
  }

  /**
   * Requests a graceful shutdown. After calling this method, the caller should complete the shutdown process
   * by calling {@link Consul#destroy}, optionally followed by {@link LeaderElectionTask#awaitTermination}.
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

  public void awaitTermination() throws InterruptedException {
    if (!closed) {
      throw new IllegalStateException("must call close() first");
    }
    LOGGER.info("Waiting for thread {} to terminate.", thread.getName());
    thread.join();
    LOGGER.info("Thread {} terminated.", thread.getName());
  }
}
