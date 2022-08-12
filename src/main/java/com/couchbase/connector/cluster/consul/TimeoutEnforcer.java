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

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

class TimeoutEnforcer {
  private final long startNanos = System.nanoTime();
  private final long timeoutNanos;
  private final String description;

  private TimeoutEnforcer(String description, long timeoutNanos) {
    checkArgument(timeoutNanos >= 0, "timeout must be positive");
    this.timeoutNanos = timeoutNanos;
    this.description = requireNonNull(description);
  }

  /**
   * @param timeout nullable (for no limit)
   */
  public TimeoutEnforcer(String description, Duration timeout) {
    this(description, toNanos(timeout, Long.MAX_VALUE));
  }

  public long remaining(TimeUnit timeUnit) throws TimeoutException {
    final long elapsed = System.nanoTime() - startNanos;
    final long nanosLeft = timeoutNanos - elapsed;

    if (nanosLeft <= 0) {
      throw new TimeoutException(description + " timed out after " + timeoutNanos + "ns");
    }

    return convertRoundUp(nanosLeft, NANOSECONDS, timeUnit);
  }

  public void throwIfExpired() throws TimeoutException {
    remaining(TimeUnit.SECONDS);
  }

  public void throwIfExpiredUnchecked() {
    try {
      remaining(TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  private static long convertRoundUp(long sourceDuration, TimeUnit sourceUnit, TimeUnit destUnit) {
    checkArgument(sourceDuration >= 0, "Duration must be non-negative");
    long nanos = sourceUnit.toNanos(sourceDuration);
    nanos += destUnit.toNanos(1) - 1;
    if (nanos < 0) {
      nanos = Long.MAX_VALUE;
    }
    return destUnit.convert(nanos, NANOSECONDS);
  }

  private static long toNanos(Duration d) {
    try {
      return d.toNanos();
    } catch (ArithmeticException e) {
      return d.isNegative() ? Long.MIN_VALUE : Long.MAX_VALUE;
    }
  }

  private static long toNanos(Duration d, long defaultValue) {
    return d == null ? defaultValue : toNanos(d);
  }
}
