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
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.utils.ConnectionString;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.util.features.Version;
import com.couchbase.connector.config.common.CouchbaseConfig;
import com.couchbase.connector.elasticsearch.Metrics;
import com.couchbase.connector.util.ThrowableHelper;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.couchbase.connector.elasticsearch.io.MoreBackoffPolicies.truncatedExponentialBackoff;
import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CouchbaseHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseHelper.class);

  private CouchbaseHelper() {
    throw new AssertionError("not instantiable");
  }

  public static DefaultCouchbaseEnvironment.Builder environmentBuilder(CouchbaseConfig config, Supplier<KeyStore> keystore) {
    final DefaultCouchbaseEnvironment.Builder envBuilder = DefaultCouchbaseEnvironment.builder();
    if (config.secureConnection()) {
      envBuilder
          .sslEnabled(true)
          .sslTruststore(keystore.get());
    }

    // Dirty kludge to allow non-standard bootstrap port for containerized Couchbase
    final ConnectionString c = ConnectionString.fromHostnames(config.hosts());
    for (InetSocketAddress host : c.hosts()) {
      final int port = host.getPort();
      if (port != 0) {
        LOGGER.debug("Using bootstrap port {}", port);
        envBuilder.bootstrapHttpDirectPort(port);
        break;
      }
    }

    return envBuilder;
  }

  public static CouchbaseCluster createCluster(CouchbaseConfig config, CouchbaseEnvironment couchbaseEnvironment) {
    final CouchbaseCluster cluster = CouchbaseCluster.create(couchbaseEnvironment, config.hosts());
    cluster.authenticate(config.username(), config.password());
    return cluster;
  }

  /**
   * CAVEAT: Only suitable for one-shot command-line tools, since this method
   * leaks a couchbase environment. That's bad, right?
   *
   * @deprecated Leaks the environment. Probably shouldn't use this.
   */
  @Deprecated
  public static Bucket openBucket(CouchbaseConfig config, Supplier<KeyStore> keystore) {
    // xxx there's no way to shut down this environment :-/
    final CouchbaseEnvironment env = environmentBuilder(config, keystore).build();
    return createCluster(config, env).openBucket(config.bucket());
  }

  public static CouchbaseBucketConfig getBucketConfig(Bucket bucket) {
    final GetClusterConfigResponse response = (GetClusterConfigResponse) bucket.core().send(
        new GetClusterConfigRequest()).toBlocking().single();
    return (CouchbaseBucketConfig) response.config().bucketConfig(bucket.name());
  }

  /**
   * Opens the bucket, retrying connection failures until the operation succeeds.
   * Any other kind of exception is propagated.
   */
  public static Bucket waitForBucket(CouchbaseCluster cluster, String bucket) throws InterruptedException {
    final Iterator<TimeValue> retryDelays = truncatedExponentialBackoff(
        TimeValue.timeValueSeconds(1), TimeValue.timeValueMinutes(1)).iterator();

    while (true) {
      try {
        return cluster.openBucket(bucket);
      } catch (Exception e) {
        if (!ThrowableHelper.hasCause(e,
            ConnectException.class, RequestCancelledException.class, TemporaryFailureException.class)) {
          LOGGER.warn("Failed to open bucket", e);
          throw e;
        }
        final TimeValue delay = retryDelays.next();
        LOGGER.debug("failed to open bucket", e);
        LOGGER.warn("Couchbase connection failure, couldn't open bucket. Retrying in {}", delay);
        MILLISECONDS.sleep(delay.millis());
      }
    }
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
  public static Gauge<String> registerCouchbaseVersionGauge(CouchbaseCluster cluster) {
    return Metrics.gauge("couchbaseVersion", () -> () -> {
      final ClusterInfo couchbaseClusterInfo = cluster.clusterManager().info(2, TimeUnit.SECONDS);
      return couchbaseClusterInfo.getMinVersion().toString();
    });
  }

  public static Version requireCouchbaseVersion(CouchbaseCluster cluster, Version requiredVersion) {
    final Version actualVersion = cluster.clusterManager().info().getMinVersion();
    if (actualVersion.compareTo(requiredVersion) < 0) {
      throw new RuntimeException("Couchbase Server version " + requiredVersion + " or later required; actual version is " + actualVersion);
    }
    return actualVersion;
  }
}
