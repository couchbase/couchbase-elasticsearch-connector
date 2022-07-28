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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.shaded.com.google.common.base.Stopwatch;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.couchbase.connector.testcontainers.ExecUtils.execInContainerUnchecked;
import static java.util.Objects.requireNonNull;

public class CouchbaseOps {
  private static final Logger log = LoggerFactory.getLogger(CouchbaseOps.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  private final GenericContainer<?> container;
  private final String username;
  private final String password;
  private final String hostname;

  private final OkHttpClient httpClient = new OkHttpClient();

  public CouchbaseOps(GenericContainer<?> container, String hostname) {
    this.container = requireNonNull(container);
    this.username = "Administrator";
    this.password = "password";
    this.hostname = requireNonNull(hostname);
  }

  private void serverAdd(GenericContainer<?> newNode, String newNodeHostname) {
    execOrDie("couchbase-cli", "server-add",
        "--cluster", hostname,
        "--username=" + username,
        "--password=" + password,
        "--server-add=" + newNodeHostname,
        "--server-add-username=" + username,
        "--server-add-password=" + password);
  }

  public void createBucket(String bucketName, int bucketQuotaMb, int replicas, Set<String> servicesToWaitFor) {
    Stopwatch timer = Stopwatch.createStarted();

    execOrDie("couchbase-cli", "bucket-create",
        "--cluster", "127.0.0.1",
        "--username", username,
        "--password", password,
        "--bucket", bucketName,
        "--bucket-ramsize", String.valueOf(bucketQuotaMb),
        "--bucket-type", "couchbase",
        "--bucket-replica", String.valueOf(replicas)
        , "--wait"
    );

    new HttpWaitStrategy()
        .forPath("/pools/default/b/" + bucketName)
        .forPort(8091)
        .withBasicCredentials(username, password)
        .forStatusCode(200)
        .forResponsePredicate(new ServicesReady(servicesToWaitFor))
        .waitUntilReady(container);

    log.info("Creating bucket took {}", timer);
  }

  public void deleteBucket(String bucketName) {
    Stopwatch timer = Stopwatch.createStarted();

    execOrDie("couchbase-cli", "bucket-delete",
        "--cluster", "127.0.0.1",
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
        "-c", "127.0.0.1",
        "-u", username,
        "-p", password);
  }

  public void failover() {
    execOrDie("couchbase-cli", "failover",
        "--cluster", "127.0.0.1:8091",
        "--username", username,
        "--password", password,
        "--server-failover", "127.0.0.1:8091");
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
    createBucket(bucketName, bucketQuotaMb, 0, Set.of("kv", "n1ql", "index"));
    String[] args = {
        "cbimport", "json",
        "--threads", "2",
        "--cluster", "127.0.0.1",
        "--username", username,
        "--password", password,
        "--bucket", bucketName,
        "--format", "sample",
        "--dataset", "file://opt/couchbase/samples/" + bucketName + ".zip"};
    execOrDie(args);

    log.info("Importing sample bucket with cbimport took {} (including bucket creation)", timer);
  }

  public Optional<Version> getVersion() {
    Container.ExecResult execResult = execInContainerUnchecked(container, "couchbase-server", "--version");
    if (execResult.getExitCode() != 0) {
      return getVersionFromDockerImageName(container);
    }
    Optional<Version> result = tryParseVersion(execResult.getStdout().trim());
    return result.isPresent() ? result : getVersionFromDockerImageName(container);
  }

  private static Optional<Version> getVersionFromDockerImageName(GenericContainer<?> couchbase) {
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
          .url("http://" + container.getHost() + ":" + container.getMappedPort(port) + path);

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

  // borrowed from CouchbaseContainer.AllServicesEnabledPredicate because it's private over there
  private static class ServicesReady implements Predicate<String> {
    private final Set<String> enabledServices;

    public ServicesReady(Set<String> services) {
      this.enabledServices = requireNonNull(services);
    }

    private static <T> Stream<T> stream(Iterator<T> iterator) {
      return StreamSupport.stream(
          Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
          false
      );
    }

    @Override
    public boolean test(final String rawConfig) {
      try {
        for (JsonNode node : mapper.readTree(rawConfig).at("/nodesExt")) {
          for (String enabledService : enabledServices) {
            Stream<String> fieldNames = stream(node.get("services").fieldNames());
            if (fieldNames.noneMatch(it -> it.startsWith(enabledService))) {
              log.info("Service '{}' not yet part of config; retrying.", enabledService);
              return false;
            }
          }
        }
        return true;
      } catch (IOException ex) {
        log.error("Unable to parse response: {}", rawConfig, ex);
        return false;
      }
    }
  }
}
