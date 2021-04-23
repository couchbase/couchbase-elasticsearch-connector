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

import com.couchbase.connector.config.common.CouchbaseConfig;
import com.couchbase.connector.config.common.GroupConfig;
import com.couchbase.connector.config.common.LoggingConfig;
import com.couchbase.connector.config.common.MetricsConfig;
import com.couchbase.connector.config.common.TrustStoreConfig;
import com.couchbase.connector.config.toml.ConfigTable;
import com.couchbase.connector.config.toml.Toml;
import org.immutables.value.Value;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.couchbase.connector.config.ConfigHelper.resolveVariables;

@Value.Immutable
public interface ConnectorConfig {

  CouchbaseConfig couchbase();

  ElasticsearchConfig elasticsearch();

  LoggingConfig logging();

  MetricsConfig metrics();

  GroupConfig group();

  TrustStoreConfig trustStore();

  static ImmutableConnectorConfig from(ConfigTable config) {
    config.expectOnly("couchbase", "elasticsearch", "logging", "metrics", "group", "truststore");

    return ImmutableConnectorConfig.builder()
        .couchbase(CouchbaseConfig.from(config.getTableOrEmpty("couchbase")))
        .elasticsearch(ElasticsearchConfig.from(config.getTableOrEmpty("elasticsearch")))
        .logging(LoggingConfig.from(config.getTableOrEmpty("logging")))
        .metrics(MetricsConfig.from(config.getTableOrEmpty("metrics")))
        .group(GroupConfig.from(config.getTableOrEmpty("group")))
        .trustStore(TrustStoreConfig.from(config.getTableOrEmpty("truststore")))
        .build();
  }

  static ImmutableConnectorConfig from(String toml) {
    return ConnectorConfig.from(Toml.parse(resolveVariables(toml)));
  }

  static ImmutableConnectorConfig from(InputStream toml) throws IOException {
    return ConnectorConfig.from(Toml.parse(resolveVariables(toml)));
  }

  static ImmutableConnectorConfig from(File toml) throws IOException {
    try (InputStream is = new FileInputStream(toml)) {
      return ConnectorConfig.from(is);
    }
  }
}
