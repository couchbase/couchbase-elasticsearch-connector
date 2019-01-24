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

import com.couchbase.connector.util.QuietPeriodExecutor;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.orbitz.consul.Consul;
import com.orbitz.consul.cache.ServiceHealthCache;
import com.orbitz.consul.model.health.ServiceHealth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Watches for the arrival and departure of service nodes.
 */
class NodeWatcher implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(NodeWatcher.class);

  private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
  private final QuietPeriodExecutor quietPeriodExecutor;

  private final ServiceHealthCache svHealth;
  private volatile ImmutableSet<String> prevEndpointIds = ImmutableSet.of();

  public NodeWatcher(Consul consul, String serviceName, Duration quietPeriod, BlockingQueue<LeaderEvent> eventQueue) {
    requireNonNull(eventQueue);
    this.quietPeriodExecutor = new QuietPeriodExecutor(quietPeriod, executorService);
    this.svHealth = ServiceHealthCache.newCache(consul.healthClient(), serviceName);

    svHealth.addListener(newValues -> {
      final ImmutableSet<String> currentEndpointIds = ImmutableSet.copyOf(
          newValues.values().stream().
              map(NodeWatcher::endpointId)
              .collect(Collectors.toSet()));

      if (!currentEndpointIds.equals(prevEndpointIds)) {

        if (LOGGER.isInfoEnabled()) {
          final Set<String> joiningNodes = Sets.difference(currentEndpointIds, prevEndpointIds);
          final Set<String> leavingNodes = Sets.difference(prevEndpointIds, currentEndpointIds);
          LOGGER.info("Service health changed; will rebalance after {} quiet period. Joining: {} Leaving: {}",
              quietPeriod, joiningNodes, leavingNodes);
        }

        prevEndpointIds = currentEndpointIds;
        quietPeriodExecutor.schedule(() -> {
          eventQueue.add(LeaderEvent.MEMBERSHIP_CHANGE);
        });
      }
    });
    svHealth.start();
  }

  private static String endpointId(ServiceHealth health) {
    return health.getNode().getNode() + "::" + health.getNode().getAddress() + "::" + health.getService().getId();
  }

  @Override
  public void close() {
    svHealth.stop();
    executorService.shutdownNow();
  }
}
