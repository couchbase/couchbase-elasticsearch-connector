/*
 * Copyright 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connector.elasticsearch;

import com.couchbase.client.java.Bucket;
import com.couchbase.connector.config.common.ImmutableCouchbaseConfig;
import com.couchbase.connector.config.common.ImmutableGroupConfig;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.config.es.ImmutableConnectorConfig;

import java.util.function.Function;

/**
 * A wrapper around an immutable connector config, with helper methods
 * for transforming nested properties.
 */
public class PatchableConfig {
  private final ImmutableConnectorConfig config;

  private PatchableConfig(ConnectorConfig config) {
    this.config = ImmutableConnectorConfig.copyOf(config);
  }

  public static PatchableConfig from(ConnectorConfig config) {
    return new PatchableConfig(config);
  }

  public PatchableConfig withBucket(String bucketName) {
    return withCouchbase(couchbase -> couchbase
        .withBucket(bucketName)
        .withMetadataBucket(bucketName)
    );
  }

  public PatchableConfig withBucket(Bucket bucket) {
    return withBucket(bucket.name());
  }

  public PatchableConfig withCouchbase(Function<ImmutableCouchbaseConfig, ImmutableCouchbaseConfig> transformer) {
    return new PatchableConfig(
        config.withCouchbase(transformer.apply(ImmutableCouchbaseConfig.copyOf(config.couchbase())))
    );
  }

  public PatchableConfig withGroup(Function<ImmutableGroupConfig, ImmutableGroupConfig> transformer) {
    return new PatchableConfig(
        config.withGroup(transformer.apply(ImmutableGroupConfig.copyOf(config.group())))
    );
  }

  public ImmutableConnectorConfig toConfig() {
    return config;
  }
}
