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

package com.couchbase.connector.config.common;

import net.consensys.cava.toml.TomlTable;
import org.elasticsearch.common.unit.TimeValue;
import org.immutables.value.Value;

import static com.couchbase.connector.config.ConfigHelper.expectOnly;
import static com.couchbase.connector.config.ConfigHelper.getIntInRange;
import static com.couchbase.connector.config.ConfigHelper.getTime;

@Value.Immutable
public interface MetricsConfig {
  TimeValue logInterval();

  int httpPort();

  static ImmutableMetricsConfig from(TomlTable config) {
    expectOnly(config, "logInterval", "httpPort");

    return ImmutableMetricsConfig.builder()
        .logInterval(getTime(config, "logInterval").orElse(TimeValue.timeValueMinutes(1)))
        .httpPort(getIntInRange(config, "httpPort", -1, 65535).orElse(-1))
        .build();
  }
}
