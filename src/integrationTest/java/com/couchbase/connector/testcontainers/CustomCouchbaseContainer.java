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
import com.jayway.jsonpath.JsonPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.shaded.com.google.common.base.Stopwatch;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static com.couchbase.connector.testcontainers.ExecUtils.exec;
import static com.couchbase.connector.testcontainers.ExecUtils.execOrDie;
import static com.couchbase.connector.testcontainers.Poller.poll;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CustomCouchbaseContainer extends CouchbaseContainer {
  private static final Logger log = LoggerFactory.getLogger(CustomCouchbaseContainer.class);

  public CustomCouchbaseContainer(String containerName) {
    super(containerName);
  }

  public static CustomCouchbaseContainer newCouchbaseCluster(String dockerImageName) {
    CustomCouchbaseContainer couchbase = new CustomCouchbaseContainer(dockerImageName);
    couchbase
        .withQuery(false)
        .withIndex(false)
        .withPrimaryIndex(false);
    couchbase.start();
    couchbase.initCluster();
    return couchbase;
  }

  public void loadSampleBucket(String bucketName, int bucketQuotaMb) {
    Stopwatch timer = Stopwatch.createStarted();

//    execOrDie(this, "curl" +
//        " -u Administrator:password" +
//        " -X POST http://127.0.0.1:8091/sampleBuckets/install" +
//        " -d '[\"travel-sample\"]'");

    ExecUtils.ExecResultWithExitCode result = exec(this, "cbdocloader" +
        // " --verbose" +
        " --cluster localhost:8091" +
        " --username Administrator" +
        " --password password" +
        " --bucket " + bucketName +
        " --bucket-quota " + bucketQuotaMb +
        " --dataset /opt/couchbase/samples/" + bucketName + ".zip");

    // Query and index services must be present to avoid this warning. We don't need those services.
    if (result.getExitCode() != 0 && !result.getStdout().contains("Errors occurred during the index creation phase")) {
      throw new UncheckedIOException(new IOException("Failed to load sample bucket: " + result));
    }

    System.out.println("Importing sample bucket took " + timer);
  }

  public void createBucket(String bucketName) {
    createBucket(bucketName, 100, 0);
  }

  public void createBucket(String bucketName, int bucketQuotaMb, int replicas) {
    Stopwatch timer = Stopwatch.createStarted();

    execOrDie(this, "couchbase-cli bucket-create" +
        " --cluster localhost:8091" +
        " --username Administrator" +
        " --password password" +
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
        " --cluster localhost:8091" +
        " --username Administrator" +
        " --password password" +
        " --bucket " + bucketName);

    log.info("Deleting bucket took " + timer);
  }

  public void waitForReadyState() throws TimeoutException, InterruptedException {
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
    return execOrDie(this, "curl -sS http://localhost:8091/" + path + " -u Administrator:password")
        .getStdout();
  }

  public String getVersionString() {
    return getVersion().map(Version::toString).orElse("unknown");
  }

  public Optional<Version> getVersion() {
    ExecUtils.ExecResultWithExitCode execResult = exec(this, "couchbase-server --version");
    if (execResult.getExitCode() != 0) {
      return getVersionFromDockerImageName(this);
    }
    Optional<Version> result = tryParseVersion(execResult.getStdout().trim());
    return result.isPresent() ? result : getVersionFromDockerImageName(this);
  }

  private static int indexOfFirstDigit(final String s) {
    for (int i = 0; i < s.length(); i++) {
      if (Character.isDigit(s.charAt(i))) {
        return i;
      }
    }
    return -1;
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

  private static Optional<Version> getVersionFromDockerImageName(CouchbaseContainer couchbase) {
    final String imageName = couchbase.getDockerImageName();
    final int tagDelimiterIndex = imageName.indexOf(':');
    return tagDelimiterIndex == -1 ? Optional.empty() : tryParseVersion(imageName.substring(tagDelimiterIndex + 1));
  }
}
