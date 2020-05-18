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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URL;
import java.util.Optional;

import static com.couchbase.connector.elasticsearch.DockerHelper.getDockerHost;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CustomCouchbaseContainer extends SocatCouchbaseContainer {
  private static final Logger log = LoggerFactory.getLogger(CustomCouchbaseContainer.class);

  private final CouchbaseOps ops;

  public CustomCouchbaseContainer(String containerName) {
    super(containerName);
    this.ops = new CouchbaseOps(this, "localhost");
  }

  public static CustomCouchbaseContainer newCouchbaseCluster(String dockerImageName) {
    // The Java client will bootstrap against port 8091 even if there's no such address in the connection string.
    checkState(!hostIsRunningCouchbase(), "Can't run integration tests while Couchbase Server is listening on " + getDockerHost() + ":8091");

    CustomCouchbaseContainer couchbase = new CustomCouchbaseContainer(dockerImageName);
    couchbase.start();
    couchbase.initCluster();

    try {
      // Not the most sophisticated wait strategy, but we can refine it later
      SECONDS.sleep(10);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return couchbase;
  }

  private static boolean hostIsRunningCouchbase() {
    try (InputStream is = new URL("http://" + getDockerHost() + ":8091").openStream()) {
      return true;
    } catch (ConnectException e) {
      return false;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void loadSampleBucket(String bucketName, int bucketQuotaMb) {
    ops.loadSampleBucket(bucketName, bucketQuotaMb);
  }

  public void createBucket(String bucketName) {
    createBucket(bucketName, 100, 0);
  }

  public void createBucket(String bucketName, int bucketQuotaMb, int replicas) {
    ops.createBucket(bucketName, bucketQuotaMb, replicas);
  }

  public void deleteBucket(String bucketName) {
    ops.deleteBucket(bucketName);
  }

  public String getVersionString() {
    return getVersion().map(Version::toString).orElse("unknown");
  }

  public Optional<Version> getVersion() {
    return ops.getVersion();
  }
}
