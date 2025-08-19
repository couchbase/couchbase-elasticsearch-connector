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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CertificateAuthenticator;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.PropertyLoader;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import com.couchbase.client.core.topology.CouchbaseBucketTopology;
import com.couchbase.client.core.util.ConnectionString;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.connector.config.ConfigException;
import com.couchbase.connector.config.ScopeAndCollection;
import com.couchbase.connector.config.common.ClientCertConfig;
import com.couchbase.connector.config.common.CouchbaseConfig;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.util.SeedNodeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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

  public static Consumer<ClusterEnvironment.Builder> environmentConfigurator(ConnectorConfig connectorConfig) {
    return env -> configure(env, connectorConfig);
  }

  private static void configure(ClusterEnvironment.Builder env, ConnectorConfig connectorConfig) {
    CouchbaseConfig config = connectorConfig.couchbase();
    env
        .ioConfig(io -> io.networkResolution(config.network()))
        .timeoutConfig(timeout -> timeout.connectTimeout(Duration.ofSeconds(10)))
        .securityConfig(security -> {
          if (config.secureConnection()) {
            security
                .enableTls(true)
                .enableHostnameVerification(config.hostnameVerification());

            if (!config.caCert().isEmpty()) {
              // The user specified a CA cert specifically for Couchbase. Trust it and no others!
              security.trustCertificates(config.caCert());
            } else {
              // Use the keystore from the DEPRECATED `[truststore]` config section if present.
              // Otherwise, don't specify a trust store, and let the SDK use the default CA certificates.
              connectorConfig.trustStore().ifPresent(it -> security.trustStore(it.get()));
            }
          }
        });

    applyCustomEnvironmentProperties(env, config);
  }

  private static void applyCustomEnvironmentProperties(ClusterEnvironment.Builder envBuilder, CouchbaseConfig config) {
    try {
      envBuilder.load(PropertyLoader.fromMap(config.env()));
    } catch (Exception e) {
      throw new ConfigException("Failed to apply Couchbase environment properties; " + e.getMessage());
    }
  }

  /**
   * Transforms the given list of addresses by replacing any unqualified ports
   * (like "example.com:12345") with the qualified version using the given
   * default port type. For example, if the port type is MANAGER, then
   * "example.com:12345" becomes "example.com:12345=manager".
   * <p>
   * Ports that are already qualified are not modified.
   * Addresses without ports are not modified.
   */
  static List<String> qualifyPorts(List<String> hosts, ConnectionString.PortType defaultPortType) {
    return ConnectionString.create(String.join(",", hosts))
        .hosts()
        .stream()
        .map(s -> {
          HostAndPort hap = new HostAndPort(s.host(), s.port());
          if (s.port() == 0) {
            return hap.format();
          }
          ConnectionString.PortType portType = s.portType().orElse(defaultPortType);
          return hap.format() + "=" + getAlias(portType);

        })
        .collect(Collectors.toList());
  }

  static String getAlias(ConnectionString.PortType portType) {
    return portType.name().toLowerCase(Locale.ROOT);
  }

  public static Cluster createCluster(ConnectorConfig config) {
    return createCluster(config.couchbase(), environmentConfigurator(config));
  }

  public static Cluster createCluster(CouchbaseConfig config, Consumer<ClusterEnvironment.Builder> env) {
    List<String> hosts = config.hosts();

    // For compatibility with previous 4.2.x versions of the connector,
    // interpret unqualified ports as MANAGER ports instead of KV ports.
    hosts = qualifyPorts(hosts, ConnectionString.PortType.MANAGER);

    String connectionString = String.join(",", hosts);
    Authenticator authenticator = authenticator(config);

    return Cluster.connect(connectionString, clusterOptions(authenticator)
        .environment(env));
  }

  private static Authenticator authenticator(CouchbaseConfig config) {
    ClientCertConfig clientCert = config.clientCert();
    return clientCert.use()
        ? CertificateAuthenticator.fromKeyStore(clientCert.getKeyStore(), clientCert.password())
        : PasswordAuthenticator.create(config.username(), config.password());
  }

  /**
   * CAVEAT: Only suitable for one-shot command-line tools, since this method
   * leaks a Cluster. That's bad, right?
   *
   * @deprecated Leaks the cluster. Probably shouldn't use this.
   */
  @Deprecated
  public static Bucket openMetadataBucket(ConnectorConfig config) {
    // xxx caller has no way of shutting down this cluster :-/
    Cluster cluster = createCluster(config.couchbase(), environmentConfigurator(config));
    return cluster.bucket(config.couchbase().metadataBucket());
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

  public static BucketConfig getConfig(Bucket bucket, Duration timeout) {
    return getConfig(bucket).block(timeout);
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

  public static Mono<ClusterTopologyWithBucket> getTopology(Core core, String bucketName) {
    return core
        .configurationProvider()
        .configs()
        .flatMap(clusterConfig ->
            Mono.justOrEmpty(clusterConfig.bucketTopology(bucketName)))
        .filter(CouchbaseHelper::hasPartitionInfo)
        .next();
  }

  /**
   * Returns true unless the topology is from a newly-created bucket
   * whose partition count is not yet available.
   */
  private static boolean hasPartitionInfo(ClusterTopologyWithBucket topology) {
    CouchbaseBucketTopology bucketTopology = (CouchbaseBucketTopology) topology.bucket();
    return bucketTopology.numberOfPartitions() > 0;
  }

  public static Mono<ClusterTopologyWithBucket> getTopology(Bucket bucket) {
    return getTopology(bucket.core(), bucket.name());
  }

  public static ClusterTopologyWithBucket getTopology(Bucket bucket, Duration timeout) {
    return getTopology(bucket).block(timeout);
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

  public static Set<SeedNode> getKvNodes(CouchbaseConfig config, Bucket bucket) {
    ConnectionString connectionString = ConnectionString.create(String.join(",", config.hosts()));
    Duration timeout = Duration.ofSeconds(30);
    return SeedNodeHelper.getKvNodes(bucket, connectionString, config.secureConnection(), config.network(), timeout);
  }

  /**
   * Opens the bucket, retrying connection failures until the operation succeeds.
   * Any other kind of exception is propagated.
   */
  public static Bucket waitForBucket(Cluster cluster, String bucketName) {
    Bucket bucket = cluster.bucket(bucketName);
    Duration timeout = bucket.environment().timeoutConfig().connectTimeout();

    // Multiplying timeout by 2 as a temporary workaround for JVMCBC-817
    // (giving the config loader time for the KV attempt to timeout and still leaving
    // time for loading the config from the manager).
    timeout = timeout.multipliedBy(2);

    bucket.waitUntilReady(timeout, waitUntilReadyOptions()
        .serviceTypes(setOf(ServiceType.KV)));

    return bucket;
  }

  public static Collection getMetadataCollection(Bucket metadataBucket, CouchbaseConfig config) {
    ScopeAndCollection c = config.metadataCollection();
    return metadataBucket
        .scope(c.getScope())
        .collection(c.getCollection());
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
}
