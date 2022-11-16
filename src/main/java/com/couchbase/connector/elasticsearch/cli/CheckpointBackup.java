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

import com.couchbase.client.java.Bucket;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.dcp.Checkpoint;
import com.couchbase.connector.dcp.CheckpointDao;
import com.couchbase.connector.dcp.CouchbaseCheckpointDao;
import com.couchbase.connector.dcp.CouchbaseHelper;
import com.couchbase.connector.dcp.ResolvedBucketConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.couchbase.connector.dcp.CouchbaseHelper.getBucketConfig;
import static com.couchbase.connector.dcp.CouchbaseHelper.getMetadataCollection;
import static java.util.stream.Collectors.toSet;

public class CheckpointBackup extends AbstractCliCommand {
  private static final Logger LOGGER = LoggerFactory.getLogger(CheckpointBackup.class);

  private static class OptionsParser extends CommonParser {
    final OptionSpec<File> outputFile = parser.accepts("output", "Checkpoint file to create. Pro tip: On Unix-like systems, include a timestamp like: checkpoint-$(date -u +%Y-%m-%dT%H:%M:%SZ).json")
        .withRequiredArg().ofType(File.class).describedAs("checkpoint.json").required();

    // final OptionSpec<Void> forceOverwrite = parser.accepts("force", "Allows overwriting an existing output file.");
  }

  public static void main(String[] args) throws Exception {
    final OptionsParser parser = new OptionsParser();
    final OptionSet options = parser.parse(args);

    final File configFile = options.valueOf(parser.configFile);
    final File outputFile = options.valueOf(parser.outputFile);

//    if (outputFile.exists() && !options.has(parser.forceOverwrite)) {
//      System.err.println("ERROR: Must specify the --force option if you wish to overwrite the existing file at "
//          + outputFile.getAbsolutePath());
//      System.exit(1);
//    }

    System.out.println("Reading connector configuration from " + configFile.getAbsoluteFile());
    final ConnectorConfig config = ConnectorConfig.from(configFile);
    backup(config, outputFile);
  }

  public static void backup(ConnectorConfig config, File outputFile) throws IOException {
    final Bucket bucket = CouchbaseHelper.openMetadataBucket(config);
    final ResolvedBucketConfig bucketConfig = getBucketConfig(config.couchbase(), bucket);
    final String bucketUuid = ""; // don't care bucketConfig.uuid();

    final CheckpointDao checkpointDao = new CouchbaseCheckpointDao(
        getMetadataCollection(bucket, config.couchbase()),
        config.group().name());

    final int numVbuckets = bucketConfig.numberOfPartitions();
    final Set<Integer> vbuckets = IntStream.range(0, numVbuckets).boxed().collect(toSet());

    final Map<Integer, Checkpoint> checkpoints = checkpointDao.load(bucketUuid, vbuckets);

    final Map<String, Object> output = new LinkedHashMap<>();
    output.put("formatVersion", 1);
    output.put("bucketUuid", bucketConfig.uuid());
    output.put("vbuckets", checkpoints);

    atomicWrite(outputFile, tempFile -> {
      try (FileOutputStream out = new FileOutputStream(tempFile)) {
        new ObjectMapper().writeValue(out, output);
      }
    });

    System.out.println("Wrote checkpoint for connector '" + config.group().name() + "' to file " + outputFile.getAbsolutePath());
  }

  @FunctionalInterface
  public interface FileConsumer {
    void accept(File f) throws IOException;
  }

  /**
   * Creates a temporary file and passes it to the given {@link FileConsumer} for writing,
   * then moves the temporary file into the desired location. Useful to guarantee the output
   * file is written completely (otherwise an error during the write might leave the
   * output file in a weird state).
   */
  public static void atomicWrite(File destFile, FileConsumer tempFileWriter) throws IOException {
    final File outputDir = destFile.getAbsoluteFile().getParentFile();
    if (!outputDir.exists() && !outputDir.mkdirs()) {
      throw new IOException("Failed to create output directory: " + outputDir);
    }

    final File temp = File.createTempFile(destFile.getName(), null, outputDir);
    try {
      tempFileWriter.accept(temp);
      Files.move(temp.toPath(), destFile.toPath(), StandardCopyOption.ATOMIC_MOVE);

    } catch (Exception t) {
      if (temp.exists() && !temp.delete()) {
        LOGGER.warn("Failed to delete temp file: {}", temp);
      }
      throw t;
    }
  }
}
