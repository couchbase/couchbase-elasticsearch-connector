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

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.dcp.Checkpoint;
import com.couchbase.connector.dcp.CheckpointDao;
import com.couchbase.connector.dcp.CouchbaseCheckpointDao;
import com.couchbase.connector.dcp.CouchbaseHelper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static com.couchbase.connector.dcp.CouchbaseHelper.getBucketConfig;

public class CheckpointRestore extends AbstractCliCommand {

  private static class OptionParser extends CommonParser {
    final OptionSpec<File> inputFile = parser.accepts("input", "Checkpoint file to restore").withRequiredArg().ofType(File.class).describedAs("checkpoint.json").required();
  }

  public static void main(String[] args) throws Exception {
    final OptionParser parser = new OptionParser();
    final OptionSet options = parser.parse(args);

    final File configFile = options.valueOf(parser.configFile);
    System.out.println("Reading connector configuration from " + configFile.getAbsoluteFile());
    final ConnectorConfig config = ConnectorConfig.from(configFile);
    final File inputFile = options.valueOf(parser.inputFile);

    restore(config, inputFile);
  }

  public static void restore(ConnectorConfig config, File inputFile) throws IOException {
    final Bucket bucket = CouchbaseHelper.openBucket(config.couchbase(), config.trustStore());
    final CouchbaseBucketConfig bucketConfig = getBucketConfig(bucket);

    final CheckpointDao checkpointDao = new CouchbaseCheckpointDao(bucket, config.group().name());

    final ObjectMapper mapper = new ObjectMapper();
    try (InputStream is = new FileInputStream(inputFile)) {
      final JsonNode json = mapper.readTree(is);
      final int formatVersion = json.path("formatVersion").intValue();
      final String bucketUuid = json.path("bucketUuid").asText();
      if (formatVersion != 1) {
        throw new IllegalArgumentException("Unrecognized checkpoint format version: " + formatVersion);
      }
      if (!bucketUuid.equals(bucketConfig.uuid())) {
        throw new IllegalArgumentException("Bucket UUID mismatch; checkpoint is from a bucket with UUID "
            + bucketUuid + " but this bucket has UUID " + bucketConfig.uuid());
      }

      final Map<Integer, Checkpoint> checkpoints = mapper.convertValue(json.get("vbuckets"),
          new TypeReference<Map<Integer, Checkpoint>>() {
          });

      if (checkpoints.size() != bucketConfig.numberOfPartitions()) {
        throw new IllegalArgumentException("Bucket has " + bucketConfig.numberOfPartitions()
            + " vbuckets but the checkpoint file has " + checkpoints.size()
            + " -- is it from a different operating system (for example, macOS vs Linux)?");
      }

      checkpointDao.save("", checkpoints);
    }

    System.out.println("Restored checkpoint for connector '" + config.group().name() + "' from file " + inputFile);
  }
}
