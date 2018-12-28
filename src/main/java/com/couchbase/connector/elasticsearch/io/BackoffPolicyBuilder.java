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

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.common.unit.TimeValue;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class BackoffPolicyBuilder {
  private BackoffPolicy policy;

  public static BackoffPolicyBuilder truncatedExponentialBackoff(TimeValue seed, TimeValue cap) {
    return new BackoffPolicyBuilder(MoreBackoffPolicies.truncatedExponentialBackoff(seed, cap));
  }

  public static BackoffPolicyBuilder constantBackoff(long duration, TimeUnit unit) {
    return constantBackoff(new TimeValue(duration, unit));
  }

  public static BackoffPolicyBuilder constantBackoff(Duration duration) {
    return constantBackoff(duration.toNanos(), TimeUnit.NANOSECONDS);
  }

  public static BackoffPolicyBuilder constantBackoff(TimeValue delay) {
    return new BackoffPolicyBuilder(BackoffPolicy.constantBackoff(delay, Integer.MAX_VALUE));
  }

  public BackoffPolicyBuilder(BackoffPolicy policy) {
    this.policy = requireNonNull(policy);
  }

  public BackoffPolicyBuilder fullJitter() {
    policy = MoreBackoffPolicies.withFullJitter(policy);
    return this;
  }

  public BackoffPolicyBuilder timeout(Duration duration) {
    return timeout(duration.toNanos(), TimeUnit.NANOSECONDS);
  }

  public BackoffPolicyBuilder timeout(long duration, TimeUnit unit) {
    return timeout(new TimeValue(duration, unit));
  }

  public BackoffPolicyBuilder timeout(TimeValue timeout) {
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
