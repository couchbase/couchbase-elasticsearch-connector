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

import com.couchbase.client.dcp.config.CompressionMode;
import com.couchbase.connector.config.toml.ConfigTable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.immutables.value.Value;

import java.util.concurrent.TimeUnit;

import static com.couchbase.connector.config.ConfigHelper.getSize;
import static com.couchbase.connector.config.ConfigHelper.getTime;
import static org.elasticsearch.common.unit.ByteSizeUnit.MB;

@Value.Immutable
public interface DcpConfig {
  CompressionMode compression();

  TimeValue persistencePollingInterval();

  ByteSizeValue flowControlBuffer();

  default TimeValue connectTimeout() {
    return new TimeValue(10, TimeUnit.SECONDS);
  }

  static ImmutableDcpConfig from(ConfigTable config) {
    return ImmutableDcpConfig.builder()
        .compression(config.getBoolean("compression").orElse(true) ? CompressionMode.ENABLED : CompressionMode.DISABLED)
        .persistencePollingInterval(getTime(config, "persistencePollingInterval").orElse(new TimeValue(100, TimeUnit.MILLISECONDS)))
        .flowControlBuffer(getSize(config, "flowControlBuffer").orElse(new ByteSizeValue(128, MB)))
        .build();
  }
}
