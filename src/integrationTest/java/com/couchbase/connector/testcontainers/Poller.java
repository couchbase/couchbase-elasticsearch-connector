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

import com.couchbase.client.core.util.NanoTimestamp;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Useful for blocking the current thread until some condition is met.
 */
public class Poller {
  private Duration timeout = Duration.ofMinutes(2);
  private Duration interval = Duration.ofSeconds(2);
  private boolean timeoutIsFatal = true;

  public static Poller poll() {
    return new Poller();
  }

  private Poller() {
  }

  public Poller atInterval(Duration interval) {
    this.interval = requireNonNull(interval);
    return this;
  }

  public Poller withTimeout(Duration timeout) {
    this.timeout = requireNonNull(timeout);
    return this;
  }

  public void untilTimeExpiresOr(Supplier<Boolean> condition) throws TimeoutException, InterruptedException {
    timeoutIsFatal = false;
    until(condition);
  }

  public void until(Supplier<Boolean> condition) throws TimeoutException, InterruptedException {
    NanoTimestamp start = NanoTimestamp.now();
    Throwable suppressed = null;

    boolean first = true;

    do {
      if (first) {
        first = false;
      } else {
        MILLISECONDS.sleep(interval.toMillis());
      }

      try {
        if (condition.get()) {
          return;
        }

      } catch (Throwable t) {
        suppressed = t;
        System.out.println("Polling condition threw exception: " + t.getMessage());
      }
    } while (!start.hasElapsed(timeout));

    if (timeoutIsFatal) {
      TimeoutException t = new TimeoutException();
      if (suppressed != null) {
        t.addSuppressed(suppressed);
      }
      throw t;
    }
  }
}

