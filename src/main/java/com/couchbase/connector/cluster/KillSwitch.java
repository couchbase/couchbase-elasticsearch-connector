/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connector.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class KillSwitch {
  private static final Logger log = LoggerFactory.getLogger(KillSwitch.class);

  private static final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        t.setName("kill-switch-scheduler");
        return t;
      }
  );

  private ScheduledFuture<Void> future;
  private final Runnable deathRattle;
  private final Duration duration;

  private KillSwitch(Duration duration, Runnable deathRattle) {
    this.deathRattle = requireNonNull(deathRattle);
    this.duration = requireNonNull(duration);
    reset();
  }

  public static KillSwitch start(Duration duration, Runnable deathRattle) {
    return new KillSwitch(duration, deathRattle);
  }

  public synchronized void cancel() {
    if (future != null) {
      boolean cancelledSuccessfully = future.cancel(false);
      if (!cancelledSuccessfully) {
        log.error("Kill switch cancellation failed. Oh dear.");
      }
      future = null;
    }
  }

  public synchronized void reset() {
    cancel();

    future = executor.schedule(() -> {
      log.info("Kill switch activated.");

      //noinspection finally
      try {
        deathRattle.run();

      } catch (Throwable t) {
        log.error("Kill switch death rattle threw exception.", t);

      } finally {
        System.exit(1);
      }

      return null; // unreachable, but lets compiler infer Void

    }, duration.toNanos(), TimeUnit.NANOSECONDS);

    log.debug("Kill switch reset; will activate in {} unless reset or cancelled.", duration);
  }

}
