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

package com.couchbase.connector.util;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class QuietPeriodExecutor {
  private final Duration quietPeriod;
  private final ScheduledExecutorService executorService;
  private ScheduledFuture<?> scheduledFuture;
  private long fluxStartNanos;


  public QuietPeriodExecutor(Duration quietPeriod, ScheduledExecutorService executorService) {
    this.quietPeriod = requireNonNull(quietPeriod);
    this.executorService = requireNonNull(executorService);
  }

  /**
   * Schedules the given task for later execution, with delay equal to this executor's quiet period.
   * If another task is subsequently scheduled during the quiet period, the previous task is cancelled and
   * the quiet period is extended.
   */
  public synchronized void schedule(Runnable task) {
    requireNonNull(task);

    if (fluxStartNanos == 0) {
      fluxStartNanos = System.nanoTime();
    }
    // else check for exceptionally long flux period and log a warning / invoke a

    if (scheduledFuture != null) {
      scheduledFuture.cancel(false);
    }

    scheduledFuture = executorService.schedule(() -> {
      synchronized (QuietPeriodExecutor.this) {
        fluxStartNanos = 0;
      }
      task.run();
    }, quietPeriod.toMillis(), MILLISECONDS);
  }

  /**
   * Measures how long the current execution has been deferred due to the initial quiet period
   * plus any extensions due to other tasks being submitted during the quiet period.
   * <p>
   * The intent is to provide a metric for detecting when executions are deferred for
   * an exceptionally long period of time.
   */
  public synchronized long getFluxDuration(TimeUnit unit) {
    return fluxStartNanos == 0 ? 0 : NANOSECONDS.convert(System.nanoTime() - fluxStartNanos, unit);
  }
}
