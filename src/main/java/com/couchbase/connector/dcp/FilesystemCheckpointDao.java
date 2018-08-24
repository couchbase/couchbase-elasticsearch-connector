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

import com.couchbase.connector.elasticsearch.BucketMismatchException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class FilesystemCheckpointDao implements CheckpointDao {
  private static final Logger LOGGER = LoggerFactory.getLogger(FilesystemCheckpointDao.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  private static final String FORMAT_VERSION = "formatVersion";
  private static final String BUCKET_UUID = "bucketUuid";
  private static final String POSITIONS = "positions";

  static {
    mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true);
  }

  private final String filename;

  public FilesystemCheckpointDao(String filename) {
    this.filename = requireNonNull(filename);
  }

  @Override
  public void save(String bucketUuid, Map<Integer, Checkpoint> vbucketToCheckpoint) throws IOException {
    // Create temp file on same filesystem so atomic move is less likely to fail
    final File tempDir = new File(filename).getParentFile();

    final File temp = File.createTempFile("cbes-checkpoint-", ".tmp.json", tempDir);
    try {
      try (FileOutputStream out = new FileOutputStream(temp)) {
        mapper.writeValue(out, prepareForSerialization(bucketUuid, vbucketToCheckpoint));
      }

      Files.move(temp.toPath(), Paths.get(filename), StandardCopyOption.ATOMIC_MOVE);

    } catch (Exception t) {
      if (temp.exists() && !temp.delete()) {
        LOGGER.warn("Failed to delete temp file: {}", temp);
      }
      throw t;
    }
  }


  private static Map<String, Object> prepareForSerialization(String bucketUuid, Map<Integer, Checkpoint> streamPositions) {
    Map<String, Object> serialized = new LinkedHashMap<>();
    serialized.put(FORMAT_VERSION, 0);
    serialized.put(BUCKET_UUID, bucketUuid);
    serialized.put(POSITIONS, positionsAsList(streamPositions));
    return serialized;
  }

  private static List<Checkpoint> positionsAsList(Map<Integer, Checkpoint> streamPositions) {
    final int maxPartition = Collections.max(streamPositions.keySet());
    final List<Checkpoint> result = new ArrayList<>();
    for (int i = 0; i <= maxPartition; i++) {
      result.add(streamPositions.get(i));
    }
    return result;
  }

  @Override
  public Map<Integer, Checkpoint> load(String bucketUuid, Set<Integer> vbuckets) throws IOException {
    try (InputStream is = new FileInputStream(filename)) {
      final JsonNode json = mapper.readTree(is);

      if (!json.path(BUCKET_UUID).asText().equals(bucketUuid)) {
        // todo instead should we just log a warning and treat it like "saved state not found"?
        throw new BucketMismatchException("Bucket UUID mismatch; this is not the same bucket as before.");
      }

      final List<Checkpoint> positions = mapper.convertValue(
          json.path(POSITIONS), new TypeReference<List<Checkpoint>>() {
          });

      final Map<Integer, Checkpoint> result = new HashMap<>();
      for (int i = 0; i < positions.size(); i++) {
        if (positions.get(i) != null) {
          result.put(i, positions.get(i));
        }
      }

      result.keySet().retainAll(vbuckets);
      return result;

    } catch (FileNotFoundException e) {
      LOGGER.info("Saved stream state not found.");
      return new HashMap<>();
    }
  }

  public void clear(String bucketUuid, Set<Integer> vbuckets) {
    throw new UnsupportedOperationException();
  }
}
