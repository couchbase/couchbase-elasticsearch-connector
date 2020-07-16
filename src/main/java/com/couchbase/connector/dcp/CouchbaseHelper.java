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

package com.couchbase.connector.dcp;

import com.codahale.metrics.Gauge;
import com.couchbase.client.core.Core;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.dcp.util.Version;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.connector.config.ScopeAndCollection;
import com.couchbase.connector.config.common.CouchbaseConfig;
import com.couchbase.connector.elasticsearch.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.security.KeyStore;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

import static com.couchbase.client.core.env.IoConfig.networkResolution;
import static com.couchbase.client.core.env.SecurityConfig.enableTls;
import static com.couchbase.client.core.env.TimeoutConfig.connectTimeout;
import static com.couchbase.client.dcp.core.utils.CbCollections.setOf;
import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static com.couchbase.client.java.diagnostics.WaitUntilReadyOptions.waitUntilReadyOptions;
import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CouchbaseHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseHelper.class);
  private static final Duration CONFIG_TIMEOUT = Duration.ofSeconds(30);

  private CouchbaseHelper() {
    throw new AssertionError("not instantiable");
  }

  public static ClusterEnvironment.Builder environmentBuilder(CouchbaseConfig config, Supplier<KeyStore> keystore) {
    final ClusterEnvironment.Builder envBuilder = ClusterEnvironment.builder()
        .ioConfig(networkResolution(config.network()))
        .timeoutConfig(connectTimeout(Duration.ofSeconds(10)));

    if (config.secureConnection()) {
      envBuilder.securityConfig(enableTls(true)
          .trustStore(keystore.get()));
    }

    return envBuilder;
  }

  public static Cluster createCluster(CouchbaseConfig config, ClusterEnvironment env) {
    String hosts = String.join(",", config.hosts());
    return Cluster.connect(hosts, clusterOptions(config.username(), config.password())
        .environment(env));
  }

  /**
   * CAVEAT: Only suitable for one-shot command-line tools, since this method
   * leaks a couchbase environment. That's bad, right?
   *
   * @deprecated Leaks the environment. Probably shouldn't use this.
   */
  @Deprecated
  public static Bucket openMetadataBucket(CouchbaseConfig config, Supplier<KeyStore> keystore) {
    // xxx caller has no way of shutting down this environment :-/
    final ClusterEnvironment env = environmentBuilder(config, keystore).build();
    return createCluster(config, env).bucket(config.metadataBucket());
  }

  public static int getNumPartitions(Bucket bucket) {
    return doGetBucketConfig(bucket).numberOfPartitions();
  }

  public static int getNumPartitions(Collection collection) {
    return doGetBucketConfig(collection).numberOfPartitions();
  }

  private static CouchbaseBucketConfig doGetBucketConfig(Bucket bucket) {
    return (CouchbaseBucketConfig) getConfig(bucket)
        .block(CONFIG_TIMEOUT);
  }

  private static CouchbaseBucketConfig doGetBucketConfig(Collection collection) {
    return (CouchbaseBucketConfig) getConfig(collection)
        .block(CONFIG_TIMEOUT);
  }

  public static Mono<BucketConfig> getConfig(Bucket bucket) {
    return getConfig(bucket.core(), bucket.name());
  }

  public static Mono<BucketConfig> getConfig(Collection collection) {
    return getConfig(collection.core(), collection.bucketName());
  }

  public static Mono<BucketConfig> getConfig(Core core, String bucketName) {
    return core
        .configurationProvider()
        .configs()
        .flatMap(clusterConfig ->
            Mono.justOrEmpty(clusterConfig.bucketConfig(bucketName)))
        .filter(CouchbaseHelper::hasPartitionInfo)
        .next();
  }

  /**
   * Returns true unless the config is from a newly-created bucket
   * whose partition count is not yet available.
   */
  private static boolean hasPartitionInfo(BucketConfig config) {
    return ((CouchbaseBucketConfig) config).numberOfPartitions() > 0;
  }

  public static ResolvedBucketConfig getBucketConfig(CouchbaseConfig config, Bucket bucket) {
    CouchbaseBucketConfig bucketConfig = doGetBucketConfig(bucket);
    return new ResolvedBucketConfig(bucketConfig, config.secureConnection());
  }

  /**
   * Opens the bucket, retrying connection failures until the operation succeeds.
   * Any other kind of exception is propagated.
   */
  public static Bucket waitForBucket(Cluster cluster, String bucketName) {
    Bucket bucket = cluster.bucket(bucketName);
    bucket.waitUntilReady(bucket.environment().timeoutConfig().connectTimeout(),
        waitUntilReadyOptions().serviceTypes(setOf(ServiceType.KV)));
    return bucket;
  }

  public static Collection getMetadataCollection(Bucket metadataBucket, CouchbaseConfig config) {
    ScopeAndCollection c = config.metadataCollection();
    // Default collection requires special handling to prevent SDK 3.0.6 from trying to
    // refresh the collection map, which fails prior to Couchbase Server 7.0.
    return c.equals(ScopeAndCollection.DEFAULT)
        ? metadataBucket.defaultCollection()
        : metadataBucket.scope(c.getScope()).collection(c.getCollection());
  }

  public static Collection getMetadataCollection(Cluster cluster, CouchbaseConfig config) {
    Bucket bucket = waitForBucket(cluster, config.metadataBucket());
    return getMetadataCollection(bucket, config);
  }

  /**
   * Returns the given key with some characters appended so the new key hashes to the desired partition.
   */
  public static Optional<String> forceKeyToPartition(String key, int desiredPartition, int numPartitions) {
    checkArgument(desiredPartition < numPartitions);
    checkArgument(numPartitions > 0);
    checkArgument(desiredPartition >= 0);

    // Why use math when you can apply 💪 BRUTE FORCE!
    final int MAX_ITERATIONS = 10_000_000;

    final MarkableCrc32 crc32 = new MarkableCrc32();
    final byte[] keyBytes = (key + "#").getBytes(UTF_8);
    crc32.update(keyBytes, 0, keyBytes.length);
    crc32.mark();

    for (long salt = 0; salt < MAX_ITERATIONS; salt++) {
      crc32.reset();

      final String saltString = Long.toHexString(salt);
      for (int i = 0, max = saltString.length(); i < max; i++) {
        crc32.update(saltString.charAt(i));
      }

      final long rv = (crc32.getValue() >> 16) & 0x7fff;
      final int actualPartition = (int) rv & numPartitions - 1;

      if (actualPartition == desiredPartition) {
        return Optional.of(new String(keyBytes, UTF_8) + saltString);
      }
    }

    LOGGER.warn("Failed to force partition for {} after {} iterations.", key, MAX_ITERATIONS);
    return Optional.empty();
  }

  @SuppressWarnings("unchecked")
  public static Gauge<String> registerCouchbaseVersionGauge(Cluster cluster) {
    return Metrics.gauge("couchbaseVersion", () -> () -> {
      // this API was removed in SDK 3. Hmmm.....
//      final ClusterInfo couchbaseClusterInfo = cluster.clusterManager().info(2, TimeUnit.SECONDS);
//      return couchbaseClusterInfo.getMinVersion().toString();
      return "0.0.0";
    });
  }

  public static Version requireCouchbaseVersion(Cluster cluster, Version requiredVersion) {
    return Version.parseVersion("0.0.0");
    // this API was removed in SDK 3. Hmmm.....
//    final Version actualVersion = cluster.clusterManager().info().getMinVersion();
//    if (actualVersion.compareTo(requiredVersion) < 0) {
//      throw new RuntimeException("Couchbase Server version " + requiredVersion + " or later required; actual version is " + actualVersion);
//    }
//    return actualVersion;
  }
}
