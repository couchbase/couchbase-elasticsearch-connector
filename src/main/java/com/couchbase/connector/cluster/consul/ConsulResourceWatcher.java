/*
 * Copyright 2019 Couchbase, Inc.
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

import com.couchbase.consul.ConsulResponse;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ConsulResourceWatcher {
  private static final Logger log = LoggerFactory.getLogger(ConsulResourceWatcher.class);

  private final String wait;

  public ConsulResourceWatcher() {
    this(Duration.ofMinutes(5));
  }

  private ConsulResourceWatcher(Duration pollingInterval) {
    final long requestedPollingIntervalSeconds = MILLISECONDS.toSeconds(pollingInterval.toMillis());
    this.wait = Longs.constrainToRange(requestedPollingIntervalSeconds, 1, MINUTES.toSeconds(5)) + "s";
  }

  public ConsulResourceWatcher withPollingInterval(Duration pollingInterval) {
    return new ConsulResourceWatcher(pollingInterval);
  }

  public <T> Flux<ConsulResponse<T>> watch(
      Function<Map<String, Object>, Mono<ConsulResponse<T>>> queryParametersToRequest
  ) {
    requireNonNull(queryParametersToRequest);

    return Flux.defer(() -> {
      AtomicLong prevIndex = new AtomicLong();

      return queryParametersToRequest.apply((Map.of()))
          .expand(item -> {
            // Consul docs advise resetting index to zero if it goes backwards,
            // and sanity checking the response index is at least 1. Reference:
            //     https://www.consul.io/api-docs/features/blocking#implementation-details
            long index = item.index().orElse(0);
            if (index < 1) {
              log.warn("Consul resource index was <= 0; this is probably a bug in Consul. Recovering by assuming an index of 1.");
              index = 1;
            } else if (index < prevIndex.get()) {
              log.debug("Consul resource index went backwards; resetting to 0.");
              index = 0;
            }
            prevIndex.set(index);

            Mono<ConsulResponse<T>> expansion = queryParametersToRequest.apply(Map.of(
                "index", index,
                "wait", wait
            ));

            // If not found, delay to prevent busy loop while waiting for resource to appear.
            // Especially important when watching KV entries.
            return item.httpStatusCode() == 404
                ? Mono.delay(withFullJitter(Duration.ofSeconds(1))).then(expansion)
                : expansion;
          }).onBackpressureLatest();
    });
  }

  private static Duration withFullJitter(Duration d) {
    return Duration.ofMillis((long) (Math.random() * d.toMillis()));
  }

}
