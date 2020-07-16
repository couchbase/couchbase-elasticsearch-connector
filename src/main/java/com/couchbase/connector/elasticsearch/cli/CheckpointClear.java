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

package com.couchbase.connector.elasticsearch.cli;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.dcp.Checkpoint;
import com.couchbase.connector.dcp.CheckpointDao;
import com.couchbase.connector.dcp.CouchbaseCheckpointDao;
import com.couchbase.connector.dcp.CouchbaseHelper;
import com.couchbase.connector.dcp.DcpHelper;
import com.couchbase.connector.dcp.ResolvedBucketConfig;
import com.couchbase.connector.dcp.SnapshotMarker;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.couchbase.connector.dcp.CouchbaseHelper.createCluster;
import static com.couchbase.connector.dcp.CouchbaseHelper.environmentBuilder;
import static com.couchbase.connector.dcp.CouchbaseHelper.getBucketConfig;
import static com.couchbase.connector.dcp.DcpHelper.allPartitions;
import static java.util.stream.Collectors.toSet;

public class CheckpointClear extends AbstractCliCommand {
  private static final Logger LOGGER = LoggerFactory.getLogger(CheckpointClear.class);

  private static class OptionsParser extends CommonParser {
    final OptionSpec<Void> catchUp = parser.accepts("catch-up", "Set the checkpoint to the current state of the Couchbase bucket.");
  }

  public static void main(String[] args) throws Exception {
    final OptionsParser parser = new OptionsParser();
    final OptionSet options = parser.parse(args);

    final File configFile = options.valueOf(parser.configFile);
    System.out.println("Reading connector configuration from " + configFile.getAbsoluteFile());
    final ConnectorConfig config = ConnectorConfig.from(configFile);

    run(config, options.has(parser.catchUp));
  }

  public static void clear(ConnectorConfig config) throws IOException {
    run(config, false);
  }

  public static void catchUp(ConnectorConfig config) throws IOException {
    run(config, true);
  }

  private static void run(ConnectorConfig config, boolean catchUp) throws IOException {
    final ClusterEnvironment env = environmentBuilder(config.couchbase(), config.trustStore()).build();
    final Cluster cluster = createCluster(config.couchbase(), env);
    try {
      final Bucket metadataBucket = CouchbaseHelper.waitForBucket(cluster, config.couchbase().metadataBucket());
      final Collection metadataCollection = CouchbaseHelper.getMetadataCollection(metadataBucket, config.couchbase());
      final ResolvedBucketConfig bucketConfig = getBucketConfig(config.couchbase(), metadataBucket);

      final CheckpointDao checkpointDao = new CouchbaseCheckpointDao(metadataCollection, config.group().name());

      if (catchUp) {
        setCheckpointToNow(config, bucketConfig, checkpointDao);
        System.out.println("Set checkpoint for connector '" + config.group().name() + "' to match current state of Couchbase bucket.");

      } else {
        final int numVbuckets = bucketConfig.numberOfPartitions();
        final Set<Integer> vbuckets = IntStream.range(0, numVbuckets).boxed().collect(toSet());

        checkpointDao.clear(bucketConfig.uuid(), vbuckets);

        System.out.println("Cleared checkpoint for connector '" + config.group().name() + "'.");
      }
    } finally {
      cluster.disconnect();
      env.shutdown();
    }
  }

  private static void setCheckpointToNow(ConnectorConfig config, ResolvedBucketConfig bucketConfig, CheckpointDao checkpointDao) throws IOException {
    final Client dcpClient = DcpHelper.newClient(config.group().name(), config.couchbase(), bucketConfig, config.trustStore());
    try {
      dcpClient.connect().await();

      final int numPartitions = dcpClient.numPartitions();
      final Set<Integer> allPartitions = new HashSet<>(allPartitions(numPartitions));
      DcpHelper.getCurrentSeqnos(dcpClient, allPartitions);
      final SessionState sessionState = dcpClient.sessionState();

      final Map<Integer, Checkpoint> now = new HashMap<>();
      for (int i = 0; i < allPartitions.size(); i++) {
        PartitionState p = sessionState.get(i);
        final long seqno = p.getStartSeqno();
        now.put(i, new Checkpoint(p.getLastUuid(), seqno, new SnapshotMarker(seqno, seqno)));
      }

      checkpointDao.save("", now);

    } finally {
      dcpClient.disconnect().await();
    }
  }
}
