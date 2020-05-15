/*
 * Copyright 2020 Couchbase, Inc.
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
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.shaded.com.google.common.base.Stopwatch;

import java.util.Optional;

import static com.couchbase.connector.testcontainers.ExecUtils.execInContainerUnchecked;
import static java.util.Objects.requireNonNull;

public class CouchbaseOps {
  private static final Logger log = LoggerFactory.getLogger(CouchbaseOps.class);

  private final GenericContainer container;
  private final String username;
  private final String password;
  private final String hostname;

  private final OkHttpClient httpClient = new OkHttpClient();

  public CouchbaseOps(GenericContainer container, String hostname) {
    this.container = requireNonNull(container);
    this.username = "Administrator";
    this.password = "password";
    this.hostname = requireNonNull(hostname);
  }

  private void serverAdd(GenericContainer newNode, String newNodeHostname) {
    execOrDie("couchbase-cli", "server-add",
        "--cluster", hostname,
        "--username=" + username,
        "--password=" + password,
        "--server-add=" + newNodeHostname,
        "--server-add-username=" + username,
        "--server-add-password=" + password);
  }

  public void createBucket(String bucketName, int bucketQuotaMb, int replicas) {
    Stopwatch timer = Stopwatch.createStarted();

    execOrDie("couchbase-cli", "bucket-create",
        "--cluster", "localhost",
        "--username", username,
        "--password", password,
        "--bucket", bucketName,
        "--bucket-ramsize", String.valueOf(bucketQuotaMb),
        "--bucket-type", "couchbase",
        "--bucket-replica", String.valueOf(replicas)
        , "--wait"
    );

    log.info("Creating bucket took {}", timer);
  }

  public void deleteBucket(String bucketName) {
    Stopwatch timer = Stopwatch.createStarted();

    execOrDie("couchbase-cli", "bucket-delete",
        "--cluster", "localhost",
        "--username", username,
        "--password", password,
        "--bucket", bucketName);

    log.info("Deleting bucket took " + timer);
  }

  Container.ExecResult execOrDie(String... command) {
    return ExecUtils.execOrDie(container, command);
  }

  public void rebalance() {
    execOrDie("couchbase-cli", "rebalance",
        "-c", "localhost",
        "-u", username,
        "-p", password);
  }

  public void failover() {
    execOrDie("couchbase-cli", "failover",
        "--cluster", "localhost:8091",
        "--username", username,
        "--password", password,
        "--server-failover", "localhost:8091");
  }

  /**
   * Ensures the node refers to itself by hostname instead of IP address.
   * Doesn't really matter, but it's nice to see consistent names in the web UI's server list.
   */
  private void assignHostname(String hostname) {
    execOrDie("curl",
        "--silent",
        "--user", username + ":" + password,
        "http://127.0.0.1:8091/node/controller/rename",
        "--data", "hostname=" + hostname);
  }

  public void loadSampleBucket(String bucketName) {
    loadSampleBucket(bucketName, 100);
  }

  public void loadSampleBucket(String bucketName, int bucketQuotaMb) {
    Stopwatch timer = Stopwatch.createStarted();

    log.info("Loading sample bucket '" + bucketName + "'...");

//    Container.ExecResult result = execInContainerUnchecked(container,
//        "cbdocloader",
//        "--cluster", "localhost", // + ":8091" +
//        "--username", username,
//        "--password", password,
//        "--bucket", bucketName,
//        "--bucket-quota", String.valueOf(bucketQuotaMb),
//        "--dataset", "./opt/couchbase/samples/" + bucketName + ".zip");
//
//    // Query and index services must be present to avoid this warning. We don't need those services.
//    if (result.getExitCode() != 0 && !result.getStdout().contains("Errors occurred during the index creation phase")) {
//      throw new UncheckedIOException(new IOException("Failed to load sample bucket: " + result));
//    }
//    log.info("Importing sample bucket with cbdocloader took {}", timer);

    // cbimport is faster, but isn't always available, and fails when query & index services are absent
    createBucket(bucketName, bucketQuotaMb, 0);
    execOrDie("cbimport", "json",
        "--threads", "2",
        "--cluster", "localhost",
        "--username", username,
        "--password", password,
        "--bucket", bucketName,
        "--format", "sample",
        "--dataset", "file://opt/couchbase/samples/" + bucketName + ".zip");

    log.info("Importing sample bucket with cbimport took " + timer);
  }

  public Optional<Version> getVersion() {
    Container.ExecResult execResult = execInContainerUnchecked(container, "couchbase-server", "--version");
    if (execResult.getExitCode() != 0) {
      return getVersionFromDockerImageName(container);
    }
    Optional<Version> result = tryParseVersion(execResult.getStdout().trim());
    return result.isPresent() ? result : getVersionFromDockerImageName(container);
  }

  private static Optional<Version> getVersionFromDockerImageName(GenericContainer couchbase) {
    final String imageName = couchbase.getDockerImageName();
    final int tagDelimiterIndex = imageName.indexOf(':');
    return tagDelimiterIndex == -1 ? Optional.empty() : tryParseVersion(imageName.substring(tagDelimiterIndex + 1));
  }

  private static Optional<Version> tryParseVersion(final String versionString) {
    try {
      // We get a string like "Couchbase Server 5.5.0-2036 (EE)". The version parser
      // tolerates trailing garbage, but not leading garbage, so...
      final int actualStartIndex = indexOfFirstDigit(versionString);
      if (actualStartIndex == -1) {
        return Optional.empty();
      }
      final String versionWithoutLeadingGarbage = versionString.substring(actualStartIndex);
      final Version version = Version.parseVersion(versionWithoutLeadingGarbage);
      // builds off master branch might have version 0.0.0 :-(
      return version.major() == 0 ? Optional.empty() : Optional.of(version);
    } catch (Exception e) {
      log.warn("Failed to parse version string '{}'", versionString, e);
      return Optional.empty();
    }
  }

  private static int indexOfFirstDigit(final String s) {
    for (int i = 0; i < s.length(); i++) {
      if (Character.isDigit(s.charAt(i))) {
        return i;
      }
    }
    return -1;
  }

//  public static void main(String[] args) {
//    Stopwatch timer = Stopwatch.createStarted();
//
//    String hostname = "node1.couchbase.host";
//    CouchbaseContainer couchbase = new CouchbaseContainer()
//        .withNetworkAliases(hostname)
//        .withEnabledServices(CouchbaseService.KV, CouchbaseService.QUERY, CouchbaseService.INDEX);
//    CouchbaseOps ops = new CouchbaseOps(couchbase, hostname);
//
//    couchbase.start();
//    //ops.assignHostname(hostname);
//
//    System.out.println("*** VERSION: " + ops.getVersion());
//
//    ops.loadSampleBucket("travel-sample", 100);
//
//    Cluster cluster = Cluster.connect(
//        couchbase.getConnectionString(),
//        couchbase.getUsername(),
//        couchbase.getPassword());
//
//
////    ops.createBucket("default", 128, 0);
////    cluster.bucket("default").waitUntilReady(Duration.ofMinutes(1));
//
//
//    System.out.println("done, finished in " + timer);
//
//    // 33.79 s
//  }
//

  /**
   * Helper method to perform a request against a couchbase server HTTP endpoint.
   *
   * @param port the (unmapped) original port that should be used.
   * @param path the relative http path.
   * @param method the http method to use.
   * @param body if present, will be part of the payload.
   * @param auth if authentication with the admin user and password should be used.
   * @return the response of the request.
   */
  private Response doHttpRequest(final int port, final String path, final String method, final RequestBody body,
                                 final boolean auth) {
    try {
      Request.Builder requestBuilder = new Request.Builder()
          .url("http://" + container.getContainerIpAddress() + ":" + container.getMappedPort(port) + path);

      if (auth) {
        requestBuilder = requestBuilder.header("Authorization", Credentials.basic(username, password));
      }

      if (body == null) {
        requestBuilder = requestBuilder.get();
      } else {
        requestBuilder = requestBuilder.method(method, body);
      }

      return httpClient.newCall(requestBuilder.build()).execute();
    } catch (Exception ex) {
      throw new RuntimeException("Could not perform request against couchbase HTTP endpoint ", ex);
    }
  }
}
