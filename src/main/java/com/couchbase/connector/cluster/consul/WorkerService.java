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

import com.couchbase.connector.elasticsearch.Metrics;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.therapi.core.annotation.Remotable;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Collections;

import static java.util.concurrent.TimeUnit.SECONDS;

@Remotable("worker")
public interface WorkerService {

  class Status {
    public static Status IDLE = new Status(Collections.emptyList());

    private final ImmutableSet<Integer> vbuckets;

    public Status(@JsonProperty("vbuckets") Collection<Integer> vbuckets) {
      this.vbuckets = ImmutableSet.copyOf(vbuckets);
    }

    /**
     * Returns the set of vbuckets this worker is currently streaming.
     */
    public ImmutableSet<Integer> getVbuckets() {
      return vbuckets;
    }
  }


  void stopStreaming();

  void assignVbuckets(Collection<Integer> vbuckets);

  Status status();

  default void ping() {
  }

  default boolean ready() {
    return true;
  }

  default void fail() {
    throw new RuntimeException("Failed");
  }

  default JsonNode metrics() {
    return Metrics.toJsonNode();
  }

  default void sleep(long seconds) {
    try {
      SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
