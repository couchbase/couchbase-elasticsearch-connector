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
import com.couchbase.client.core.env.AbstractMapPropertyLoader;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CertificateAuthenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.IoEnvironment;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.ConnectionString;
import com.couchbase.client.dcp.config.HostAndPort;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.connector.config.ConfigException;
import com.couchbase.connector.config.ScopeAndCollection;
import com.couchbase.connector.config.common.ClientCertConfig;
import com.couchbase.connector.config.common.CouchbaseConfig;
import com.couchbase.connector.util.SeedNodeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.security.KeyStore;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.couchbase.client.core.env.IoConfig.networkResolution;
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

  public static ClusterEnvironment.Builder environmentBuilder(CouchbaseConfig config, Supplier<KeyStore> trustStore) {
    final ClusterEnvironment.Builder envBuilder = ClusterEnvironment.builder()
        .ioConfig(networkResolution(config.network()))
        .timeoutConfig(connectTimeout(Duration.ofSeconds(10)));

    if (config.secureConnection()) {
      envBuilder.securityConfig(SecurityConfig.builder()
          .enableTls(true)
          .trustStore(trustStore.get())
          .enableHostnameVerification(config.hostnameVerification())
      );
    }

    applyCustomEnvironmentProperties(envBuilder, config);

    return envBuilder;
  }

  private static void applyCustomEnvironmentProperties(ClusterEnvironment.Builder envBuilder, CouchbaseConfig config) {
    // Begin workaround for IoEnvironment config.
    // Prior to Java client 3.1.5, IoEnvironment isn't configurable
    // via system properties. Until then, handle it manually.
    Map<String, String> envProps = new HashMap<>(config.env());
    IoEnvironment.Builder ioEnvBuilder = IoEnvironment.builder();
    String nativeIoEnabled = envProps.remove("ioEnvironment.enableNativeIo");
    if (nativeIoEnabled != null) {
      ioEnvBuilder.enableNativeIo(Boolean.parseBoolean(nativeIoEnabled));
    }
    String eventLoopThreadCount = envProps.remove("ioEnvironment.eventLoopThreadCount");
    if (eventLoopThreadCount != null) {
      ioEnvBuilder.eventLoopThreadCount(Integer.parseInt(eventLoopThreadCount));
    }
    envBuilder.ioEnvironment(ioEnvBuilder);
    // End workaround for IoEnvironment config.

    try {
      envBuilder.load(new AbstractMapPropertyLoader<CoreEnvironment.Builder>() {
        @Override
        protected Map<String, String> propertyMap() {
          return envProps;
        }
      });
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
          HostAndPort hap = new HostAndPort(s.hostname(), s.port());
          if (s.port() == 0) {
            return hap.formatHost();
          }
          ConnectionString.PortType portType = s.portType().orElse(defaultPortType);
          return hap.format() + "=" + getAlias(portType);

        })
        .collect(Collectors.toList());
  }

  static String getAlias(ConnectionString.PortType portType) {
    return portType.name().toLowerCase(Locale.ROOT);
  }

  public static Cluster createCluster(CouchbaseConfig config, ClusterEnvironment env) {
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
    String connectionString = String.join(",", config.hosts());
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
