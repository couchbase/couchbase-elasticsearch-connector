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

package com.couchbase.connector.testcontainers;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Useful for blocking the current thread until some condition is met.
 */
public class Poller {
  private long timeout = 2;
  private TimeUnit timeoutUnit = MINUTES;

  private long interval = 2;
  private TimeUnit intervalUnit = SECONDS;

  private boolean timeoutIsFatal = true;

  public static Poller poll() {
    return new Poller();
  }

  private Poller() {
  }

  public Poller atInterval(long interval, TimeUnit unit) {
    this.interval = interval;
    this.intervalUnit = unit;
    return this;
  }

  public Poller withTimeout(long timeout, TimeUnit unit) {
    this.timeout = timeout;
    this.timeoutUnit = unit;
    return this;
  }

  public void untilTimeExpiresOr(Supplier<Boolean> condition) throws TimeoutException, InterruptedException {
    timeoutIsFatal = false;
    until(condition);
  }

  public void until(Supplier<Boolean> condition) throws TimeoutException, InterruptedException {
    final long deadline = System.nanoTime() + timeoutUnit.toNanos(timeout);

    if (condition.get()) {
      return;
    }

    do {
      if (System.nanoTime() > deadline) {
        if (timeoutIsFatal) {
          throw new TimeoutException();
        }
        return;
      }

      intervalUnit.sleep(interval);

    } while (!condition.get());
  }
}

