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

package com.couchbase.connector.config.es;

import com.couchbase.connector.config.toml.ConfigTable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.immutables.value.Value;

import java.util.concurrent.TimeUnit;

import static com.couchbase.connector.config.ConfigHelper.getSize;
import static com.couchbase.connector.config.ConfigHelper.getTime;
import static org.elasticsearch.common.unit.ByteSizeUnit.MB;

@Value.Immutable
public interface BulkRequestConfig {
  int maxActions();

  ByteSizeValue maxBytes();

  int concurrentRequests();

  TimeValue timeout();

  @Value.Check
  default void check() {
    if (concurrentRequests() <= 0) {
      throw new IllegalArgumentException("concurrentRequests must be > 0");
    }
  }

  static ImmutableBulkRequestConfig from(ConfigTable config) {
    config.expectOnly("actions", "bytes", "timeout", "concurrentRequests");
    return ImmutableBulkRequestConfig.builder()
        .maxActions(config.getInt("actions").orElse(1000))
        .maxBytes(getSize(config, "bytes").orElse(new ByteSizeValue(10, MB)))
        .timeout(getTime(config, "timeout").orElse(new TimeValue(1, TimeUnit.MINUTES)))
        .concurrentRequests(config.getIntInRange("concurrentRequests", 1, 16).orElse(2))
        .build();
  }
}
