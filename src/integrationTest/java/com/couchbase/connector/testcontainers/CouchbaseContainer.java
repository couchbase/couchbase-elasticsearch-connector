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

package com.couchbase.connector.testcontainers;


import com.couchbase.client.dcp.util.Version;
import com.couchbase.connector.testcontainers.ExecUtils.ExecResultWithExitCode;
import com.google.common.collect.Iterables;
import com.jayway.jsonpath.JsonPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.dockerfile.DockerfileBuilder;
import org.testcontainers.shaded.com.google.common.base.Stopwatch;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.couchbase.connector.testcontainers.CouchbaseContainer.CouchbaseService.CONFIG;
import static com.couchbase.connector.testcontainers.ExecUtils.exec;
import static com.couchbase.connector.testcontainers.ExecUtils.execOrDie;
import static com.couchbase.connector.testcontainers.Poller.poll;
import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Stopgap until the "official" CouchbaseContainer from the TestContainers project is available.
 */
public class CouchbaseContainer extends GenericContainer<CouchbaseContainer> {
  private static final Logger log = LoggerFactory.getLogger(CouchbaseContainer.class);
  /**
   * Represents the initialized container, ever incrementing.
   * <p>
   * Used to properly space out the exposed ports as well.
   */
  private static final AtomicInteger CONTAINER_ID = new AtomicInteger();
  private final Config config;


  private static final int CLUSTER_RAM_MB = 1024;

  private final String username;
  private final String password;
  private final String hostname;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private volatile Optional<Version> version;

  private CouchbaseContainer(Config config, String dockerImageName, String hostname, String username, String password) {
    super(new ImageFromDockerfile().withDockerfileFromBuilder(buildDockerFile(dockerImageName, config)));
    this.config = config;
    this.username = requireNonNull(username);
    this.password = requireNonNull(password);
    this.hostname = requireNonNull(hostname);

    withNetworkAliases(hostname);
    withCreateContainerCmdModifier(cmd -> cmd.withHostName(hostname));
  }


  private static Consumer<DockerfileBuilder> buildDockerFile(final String dockerImageName, final Config config) {
    return builder -> {
      builder = builder.from(dockerImageName);

      for (Map.Entry<CouchbaseService, Integer> mapping : config.portMappings().entrySet()) {
        builder = builder.run("echo '{" + mapping.getKey().configFormat() + ", " + mapping.getValue() + "}.' >> /opt/couchbase/etc/couchbase/static_config");
      }

      //builder = builder.run("sudo /etc/init.d/couchbase-server stop");
      //builder = builder.run("sudo rm /opt/couchbase/var/lib/couchbase/config/config.dat");
      //builder = builder.run("sudo /etc/init.d/couchbase-server start");

      builder.build();
    };
  }

  @Override
  protected void configure() {
    Collection<Integer> values = config.portMappings().values();
    withExposedPorts(Iterables.toArray(values, Integer.class));
    for (int port : config.portMappings().values()) {
      addFixedExposedPort(port, port);
    }
  }

  @Override
  public Set<Integer> getLivenessCheckPortNumbers() {
    // TODO: a real check for liveness of the server
    return singleton(managementPort());
  }

  public static CouchbaseContainer newCluster(String dockerImageName) {
    final String username = "Administrator";
    final String password = "password";

    log.info("Username: " + username);
    log.info("Password: " + password);

    final CouchbaseContainer couchbase = new CouchbaseContainer(new Config(), dockerImageName, "localhost", username, password);

    couchbase.start();
    couchbase.assignHostname();
    couchbase.initCluster();
    couchbase.waitForReadyState();

    return couchbase;
  }

  private void initCluster() {
    execOrDie(this, "couchbase-cli cluster-init" +
        " --cluster " + getHostname() + ":" + managementPort() +
        " --cluster-username=" + username +
        " --cluster-password=" + password +
//                " --services=data,query,index" +
//                " --cluster-index-ramsize=512" +
        " --cluster-ramsize=" + CLUSTER_RAM_MB);
  }


  private static final AtomicLong nodeCounter = new AtomicLong(2);


  public int managementPort() {
    return config.portMappings().get(CONFIG);
  }

  public void restart() {
    execOrDie(this, "sv restart couchbase-server");
    waitForReadyState();
  }

  public void waitForReadyState() {
    poll().atInterval(3, SECONDS)
        .withTimeout(2, MINUTES)
        .until(this::allNodesHealthy);
  }

  private boolean allNodesHealthy() {
    try {
      final String poolInfo = curl("pools/default");
      final List<String> nodeStatuses = JsonPath.read(poolInfo, "$.nodes[*].status");
      return nodeStatuses.stream().allMatch(status -> status.equals("healthy"));
    } catch (UncheckedIOException e) {
      return false; // This node is not responding to HTTP requests, and therefore not healthy.
    }
  }

  private String curl(String path) {
    return execOrDie(this, "curl -sS http://localhost:" + managementPort() + "/" + path + " -u " + username + ":" + password)
        .getStdout();
  }

  public void loadSampleBucket(String bucketName) {
    loadSampleBucket(bucketName, 100);
  }

  public void loadSampleBucket(String bucketName, int bucketQuotaMb) {
    Stopwatch timer = Stopwatch.createStarted();

    ExecResultWithExitCode result = exec(this, "cbdocloader" +
        " --cluster " + getHostname() + ":" + managementPort() +
        " --username " + username +
        " --password " + password +
        " --bucket " + bucketName +
        " --bucket-quota " + bucketQuotaMb +
        " --dataset ./opt/couchbase/samples/" + bucketName + ".zip");

    // Query and index services must be present to avoid this warning. We don't need those services.
    if (result.getExitCode() != 0 && !result.getStdout().contains("Errors occurred during the index creation phase")) {
      throw new UncheckedIOException(new IOException("Failed to load sample bucket: " + result));
    }

    log.info("Importing sample bucket took {}", timer);

    // cbimport is faster, but isn't always available, and fails when query & index services are absent
//        Stopwatch timer = Stopwatch.createStarted();
//        createBucket(bucketName, bucketQuotaMb);
//        exec(this, "cbimport2 json " +
//                " --cluster couchbase://" + getHostname() +
//                " --username " + username +
//                " --password " + password +
//                " --bucket " + bucketName +
//                " --format sample" +
//                " --dataset ./opt/couchbase/samples/beer-sample.zip");
//        log.info("Importing sample bucket with cbimport took " + timer);
//        return this;
  }

  public void createBucket(String bucketName) {
    createBucket(bucketName, 100, 0);
  }

  public void createBucket(String bucketName, int bucketQuotaMb, int replicas) {
    Stopwatch timer = Stopwatch.createStarted();

    execOrDie(this, "couchbase-cli bucket-create" +
        " --cluster " + getHostname() +
        " --username " + username +
        " --password " + password +
        " --bucket " + bucketName +
        " --bucket-ramsize " + bucketQuotaMb + "" +
        " --bucket-type couchbase " +
        " --bucket-replica " + replicas +
        " --wait");

    log.info("Creating bucket took " + timer);
  }

  public void deleteBucket(String bucketName) {
    Stopwatch timer = Stopwatch.createStarted();

    execOrDie(this, "couchbase-cli bucket-delete" +
        " --cluster " + getHostname() +
        " --username " + username +
        " --password " + password +
        " --bucket " + bucketName);

    log.info("Deleting bucket took " + timer);
  }

  @SuppressWarnings("OptionalAssignedToNull")
  public Optional<Version> getVersion() {
    if (this.version == null) {
      throw new IllegalStateException("Must start container before getting version.");
    }

    return this.version;
  }

  private String getHostname() {
    return hostname;
  }

  @Override
  public void start() {
    try {
      super.start();
    } catch (ContainerLaunchException e) {
      if (stackTraceAsString(e).contains("port is already allocated")) {
        e.printStackTrace();
        throw new RuntimeException("Failed to start container due to port conflict; have you stopped all other debug sessions?");
      }
    }

    try {
      this.version = VersionUtils.getVersion(this);
      Version serverVersion = getVersion().orElse(null);
      log.info("Couchbase Server (version {}) {} running at http://localhost:{}",
          serverVersion, hostname, getMappedPort(managementPort()));
    } catch (Exception e) {
      stop();
      throw new RuntimeException(e);
    }
  }

  private static String stackTraceAsString(Throwable t) {
    StringWriter w = new StringWriter();
    t.printStackTrace(new PrintWriter(w));
    return w.toString();
  }

  /**
   * Ensures the node refers to itself by hostname instead of IP address.
   * Doesn't really matter, but it's nice to see consistent names in the web UI's server list.
   */
  private void assignHostname() {
    execOrDie(this, "curl --silent --user " + username + ":" + password +
        " http://127.0.0.1:" + managementPort() + "/node/controller/rename --data hostname=" + hostname);
  }

  static class Config {
    private final int containerId = CONTAINER_ID.incrementAndGet();

    private final Map<CouchbaseService, Integer> mappings = new HashMap<>();

    public Config() {
      final String portBase = String.format("3%02d", containerId);
      mappings.put(CONFIG, Integer.parseInt(portBase + "01"));
      mappings.put(CouchbaseService.VIEW, Integer.parseInt(portBase + "02"));
      mappings.put(CouchbaseService.QUERY, Integer.parseInt(portBase + "03"));
      mappings.put(CouchbaseService.FTS, Integer.parseInt(portBase + "04"));
      mappings.put(CouchbaseService.DATA, Integer.parseInt(portBase + "06"));
      mappings.put(CouchbaseService.DATA_SSL, Integer.parseInt(portBase + "07"));
      mappings.put(CouchbaseService.MOXI, Integer.parseInt(portBase + "08"));
      mappings.put(CouchbaseService.CONFIG_SSL, Integer.parseInt(portBase + "09"));
      mappings.put(CouchbaseService.VIEW_SSL, Integer.parseInt(portBase + "10"));
      mappings.put(CouchbaseService.QUERY_SSL, Integer.parseInt(portBase + "11"));
      mappings.put(CouchbaseService.FTS_SSL, Integer.parseInt(portBase + "12"));
    }

    public Map<CouchbaseService, Integer> portMappings() {
      return mappings;
    }
  }

  enum CouchbaseService {
    CONFIG("rest_port"),
    CONFIG_SSL("ssl_rest_port"),
    VIEW("capi_port"),
    VIEW_SSL("ssl_capi_port"),
    QUERY("query_port"),
    QUERY_SSL("ssl_query_port"),
    FTS("fts_http_port"),
    FTS_SSL("fts_ssl_port"),
    DATA("memcached_port"),
    DATA_SSL("memcached_ssl_port"),
    MOXI("moxi_port");

    private final String configFormat;

    CouchbaseService(String configFormat) {
      this.configFormat = configFormat;
    }

    public String configFormat() {
      return configFormat;
    }
  }
}
