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

import com.google.common.collect.Iterables;

import java.time.Duration;
import java.util.Iterator;

import static java.util.Objects.requireNonNull;

public class BackoffPolicyBuilder {
  private BackoffPolicy policy;

  public static BackoffPolicyBuilder truncatedExponentialBackoff(Duration seed, Duration cap) {
    return new BackoffPolicyBuilder(MoreBackoffPolicies.truncatedExponentialBackoff(seed, cap));
  }

  public static BackoffPolicyBuilder constantBackoff(Duration delay) {
    BackoffPolicy policy = new BackoffPolicy() {
      public String toString() {
        return "constantBackoff(delay: " + delay + ")";
      }

      @Override
      public Iterator<Duration> iterator() {
        return Iterables.cycle(delay).iterator();
      }
    };

    return new BackoffPolicyBuilder(policy);
  }

  public BackoffPolicyBuilder(BackoffPolicy policy) {
    this.policy = requireNonNull(policy);
  }

  public BackoffPolicyBuilder fullJitter() {
    policy = MoreBackoffPolicies.withFullJitter(policy);
    return this;
  }

  public BackoffPolicyBuilder timeout(Duration timeout) {
    policy = MoreBackoffPolicies.withTimeout(timeout, policy);
    return this;
  }

  public BackoffPolicyBuilder limit(int maxRetries) {
    policy = MoreBackoffPolicies.limit(maxRetries, policy);
    return this;
  }

  public BackoffPolicy build() {
    return policy;
  }
}
