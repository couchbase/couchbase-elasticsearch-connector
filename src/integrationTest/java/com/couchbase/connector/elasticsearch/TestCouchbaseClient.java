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

package com.couchbase.connector.elasticsearch;

import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.config.es.ImmutableConnectorConfig;
import com.couchbase.connector.dcp.CouchbaseHelper;
import com.couchbase.connector.testcontainers.CustomCouchbaseContainer;
import com.google.common.io.Closer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;

import static com.couchbase.client.dcp.core.utils.CbCollections.setOf;
import static com.couchbase.client.java.diagnostics.WaitUntilReadyOptions.waitUntilReadyOptions;
import static com.couchbase.connector.dcp.CouchbaseHelper.environmentConfigurator;

class TestCouchbaseClient implements Closeable {
  private final Cluster cluster;
  private final Closer closer = Closer.create();

  public TestCouchbaseClient(String config) {
    this(ConnectorConfig.from(config));
  }

  public TestCouchbaseClient(PatchableConfig patcher) {
    this(patcher.toConfig());
  }

  public TestCouchbaseClient(ImmutableConnectorConfig config) {
    this.cluster = CouchbaseHelper.createCluster(
        config.couchbase(),
        environmentConfigurator(config).andThen(env -> env
            .ioConfig(io -> io.enableMutationTokens(true))
            .timeoutConfig(timeout -> timeout
                .connectTimeout(Duration.ofSeconds(15))
                .kvTimeout(Duration.ofSeconds(10))
            )
        )
    );
  }

  public Cluster cluster() {
    return cluster;
  }

  /**
   * Create a new bucket with a unique name. The bucket will be deleted
   * when this client is closed.
   */
  public Bucket createTempBucket(CustomCouchbaseContainer couchbase) {
    final TempBucket temp = closer.register(new TempBucket(couchbase));
    Bucket bucket = cluster().bucket(temp.name());
    Duration timeout = bucket.environment().timeoutConfig().connectTimeout();

    // Multiplying timeout by 2 as a temporary workaround for JVMCBC-817
    // (giving the config loader time for the KV attempt to timeout and still leaving
    // time for loading the config from the manager).
    timeout = timeout.multipliedBy(2);

    bucket.waitUntilReady(timeout, waitUntilReadyOptions()
        .serviceTypes(setOf(ServiceType.KV)));

    return bucket;
  }

  /**
   * Create a new bucket with the given name. The bucket will be deleted
   * when this client is closed.
   */
  public Bucket createTempBucket(CustomCouchbaseContainer couchbase, String bucketName) {
    final TempBucket temp = closer.register(new TempBucket(couchbase, bucketName));
    return cluster().bucket(temp.name());
  }

  @Override
  public void close() throws IOException {
    cluster.disconnect();
    closer.close();
  }
}
