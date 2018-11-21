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

import com.orbitz.consul.KeyValueClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LeaderElectionTask extends AbstractLongPollTask<LeaderElectionTask> {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeaderElectionTask.class);

  private final String candidateUuid;// = UUID.randomUUID().toString();
  private final Consumer<Throwable> fatalErrorConsumer;

  public LeaderElectionTask(KeyValueClient kv, String serviceName, String sessionId, Consumer<Throwable> fatalErrorConsumer) {
    super(kv, "leader-election-", serviceName, sessionId);
    requireNonNull(kv);
    requireNonNull(sessionId);
    this.candidateUuid = sessionId;
    this.fatalErrorConsumer = requireNonNull(fatalErrorConsumer);
  }

  protected void doRun(KeyValueClient kv, String serviceName, String sessionId) {
    final String leaderKey = "couchbase/cbes/" + serviceName + "/leader";
    LOGGER.info("Leader key: {}", leaderKey);

    try {
      while (!closed()) {
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
      if (closed()) {
        // Closing the task is likely to result in an InterruptedException,
        // or a ConsulException wrapping an InterruptedIOException.
        LOGGER.debug("Caught exception in leader election loop after closing. Don't panic; this is expected.", t);
      } else {
        // Something went horribly, terribly, unrecoverably wrong.
        fatalErrorConsumer.accept(t);
      }

    } finally {
      try {
        // Abdicate (delete leadership document); only succeeds if we own the lock. This lets another node acquire
        // the lock immediately. If we don't do this, the lock will be auto-released when the session ends,
        // but the lock won't be eligible for acquisition until the Consul lock delay has elapsed.

        clearInterrupted(); // so final Consul request doesn't fail with InterruptedException
        ConsulHelper.unlockAndDelete(kv, leaderKey, sessionId);

      } catch (Exception e) {
        LOGGER.warn("Failed to abdicate", e);
      }

      LOGGER.info("Exiting leader election thread for session {}", sessionId);
    }
  }

}
