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

import java.util.Optional;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LeaderElectionTask extends AbstractLongPollTask<LeaderElectionTask> {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeaderElectionTask.class);

  private final String candidateUuid;
  private final Consumer<Throwable> fatalErrorConsumer;
  private final LeaderController leaderController;

  public LeaderElectionTask(ConsulContext ctx, String sessionId, String endpointId, Consumer<Throwable> fatalErrorConsumer, LeaderController leaderController) {
    super(ctx, "leader-election-", sessionId);
    this.candidateUuid = requireNonNull(endpointId);
    this.fatalErrorConsumer = requireNonNull(fatalErrorConsumer);
    this.leaderController = requireNonNull(leaderController);
  }

  protected void doRun(ConsulContext ctx, String sessionId) {
    final String leaderKey = ctx.keys().leader();
    LOGGER.info("Leader key: {}", leaderKey);

    try {
      while (!closed()) {
        LOGGER.info("Racing to become new leader.");
        final boolean acquired = ctx.acquireLock(leaderKey, candidateUuid, sessionId);

        if (acquired) {
          LOGGER.info("Won the leader election! {}", candidateUuid);
          leaderController.startLeading();

          final Optional<String> newLeader = ctx.documentWatcher().awaitValueChange(leaderKey, candidateUuid);

          LOGGER.info("No longer the leader; new leader is {}", newLeader);
          leaderController.stopLeading();

        } else {
          LOGGER.info("Not the leader");

          // Sleeping because the attempt may have failed due to Consul's lock delay;
          // see https://www.consul.io/docs/internals/sessions.html
          SECONDS.sleep(1);

          ctx.documentWatcher().awaitAbsence(leaderKey);
        }
      }

    } catch (Throwable t) {
      if (closed()) {
        // Closing the task is likely to result in an InterruptedException
        LOGGER.debug("Caught exception in leader election loop after closing. Don't panic; this is expected.", t);
      } else {
        // Something went horribly, terribly, unrecoverably wrong.
        fatalErrorConsumer.accept(t);
      }

    } finally {
      try {
        leaderController.stopLeading();

        // Abdicate (delete leadership document); only succeeds if we own the lock. This lets another node acquire
        // the lock immediately. If we don't do this, the lock will be auto-released when the session ends,
        // but the lock won't be eligible for acquisition until the Consul lock delay has elapsed.
        ctx.runCleanup(() -> ctx.unlockAndDelete(leaderKey, sessionId));

      } catch (Exception e) {
        LOGGER.warn("Failed to abdicate", e);
      }

      LOGGER.info("Exiting leader election thread for session {}", sessionId);
    }
  }

}
