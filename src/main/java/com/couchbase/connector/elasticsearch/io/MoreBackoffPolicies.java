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

package com.couchbase.connector.elasticsearch.io;

import com.google.common.collect.Iterators;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

public class MoreBackoffPolicies {
  private MoreBackoffPolicies() {
    throw new AssertionError("not instantiable");
  }

  public static BackoffPolicy truncatedExponentialBackoff(TimeValue seedDelay, TimeValue delayCap) {
    requireNonNull(seedDelay);
    requireNonNull(delayCap);
    checkArgument(seedDelay.millis() > 0, "seed delay must be positive");

    return new BackoffPolicy() {
      public String toString() {
        return "truncatedExponentialBackoff(seed: " + seedDelay + ", cap: " + delayCap + ")";
      }

      @Override
      public Iterator<TimeValue> iterator() {
        return new Iterator<TimeValue>() {
          long delay = seedDelay.millis();

          @Override
          public boolean hasNext() {
            return true;
          }

          @Override
          public TimeValue next() {
            final long shifted = delay << 1;

            if (shifted <= 0) {
              // overflow
              return delayCap;
            }
            delay = shifted;
            return timeValueMillis(Math.min(delay, delayCap.millis()));
          }
        };
      }
    };
  }

  /**
   * Wraps the given deterministic backoff policy and transforms each delay to a random value
   * between zero and the deterministic delay.
   * <p>
   * According to
   * <a href="https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/">
   * Exponential Backoff And Jitter
   * </a> by Marc Brooker, jitter "should be considered a standard approach for remote clients."
   */
  public static BackoffPolicy withFullJitter(final BackoffPolicy deterministic) {
    requireNonNull(deterministic);

    return new BackoffPolicy() {
      public String toString() {
        return deterministic + " + fullJitter";
      }

      @Override
      public Iterator<TimeValue> iterator() {
        return Iterators.transform(deterministic.iterator(), delay ->
            timeValueMillis(ThreadLocalRandom.current().nextLong(delay.millis() + 1)));
      }
    };
  }

  /**
   * Wrap the given policy so its iterators no longer return elements after the given timeout duration
   * has elapsed. The timeout countdown starts when the iterator is created.
   */
  public static BackoffPolicy withTimeout(final TimeValue timeout, final BackoffPolicy wrapped) {
    requireNonNull(wrapped);

    return new BackoffPolicy() {
      public String toString() {
        return wrapped + " + timeout(" + timeout + ")";
      }

      @Override
      public Iterator<TimeValue> iterator() {
        final long deadline = System.nanoTime() + timeout.nanos();
        final Iterator<TimeValue> wrappedIterator = wrapped.iterator();

        return new Iterator<TimeValue>() {
          @Override
          public boolean hasNext() {
            return wrappedIterator.hasNext() && System.nanoTime() < deadline;
          }

          @Override
          public TimeValue next() {
            return wrappedIterator.next();
          }
        };
      }
    };
  }

  public static BackoffPolicy limit(int maxRetries, BackoffPolicy wrapped) {
    checkArgument(maxRetries >= 0, "maxRetries be non-negative");

    return new BackoffPolicy() {
      public String toString() {
        return wrapped + " + limit(" + maxRetries + ")";
      }

      @Override
      public Iterator<TimeValue> iterator() {
        return Iterators.limit(wrapped.iterator(), maxRetries);
      }
    };
  }
}
