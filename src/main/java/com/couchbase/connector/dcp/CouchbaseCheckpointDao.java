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

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.TemporaryFailureException;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.connector.elasticsearch.io.BackoffPolicyBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static com.couchbase.connector.dcp.CouchbaseHelper.forceKeyToPartition;
import static com.couchbase.connector.elasticsearch.io.MoreBackoffPolicies.truncatedExponentialBackoff;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;

/**
 * Stores the connector's checkpoints in Couchbase.
 */
public class CouchbaseCheckpointDao implements CheckpointDao {
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseCheckpointDao.class);

  private static final ObjectMapper mapper = new ObjectMapper();

  private final Collection collection;
  private final String[] checkpointDocumentKeys; // indexed by partition (vbucket)

  public CouchbaseCheckpointDao(Collection collection, String groupName) {
    this.collection = requireNonNull(collection);

    final int numPartitions = CouchbaseHelper.getNumPartitions(collection);
    final String keyPrefix = DcpHelper.metadataDocumentIdPrefix() + groupName + ":checkpoint:";

    // Brute-forcing the key names so they map to the correct partitions is expensive,
    // so make a lookup table.
    this.checkpointDocumentKeys = new String[numPartitions];
    for (int partition = 0; partition < numPartitions; partition++) {
      final String baseKey = keyPrefix + partition;
      checkpointDocumentKeys[partition] = forceKeyToPartition(baseKey, partition, numPartitions).orElse(baseKey);
    }
  }

  @Override
  public void save(String bucketUuid, Map<Integer, Checkpoint> vbucketToCheckpoint) {
    RuntimeException deferredException = null;

    final Iterator<Duration> retryDelays = new BackoffPolicyBuilder(truncatedExponentialBackoff(
        Duration.ofMillis(50), Duration.ofSeconds(5)))
        .fullJitter()
        .timeout(Duration.ofSeconds(5)).build().iterator();

    for (Map.Entry<Integer, Checkpoint> entry : vbucketToCheckpoint.entrySet()) {
      final int vbucket = entry.getKey();
      final Checkpoint checkpoint = entry.getValue();
      if (checkpoint == null) {
        continue;
      }

      try {
        // Could do async batch processing, but performance isn't critical.
        // Let's keep it simple for now.

        final Map<String, Object> document = new HashMap<>();
        document.put("bucketUuid", bucketUuid);
        document.put("checkpoint", checkpoint);

        while (true) {
          try {
            createDocument(documentIdForVbucket(vbucket), document);
            break;
          } catch (TemporaryFailureException e) {
            if (!retryDelays.hasNext()) {
              throw e;
            }
            final Duration retryDelay = retryDelays.next();
            LOGGER.info("Temporary failure saving checkpoint for vbucket {}, retying in {}", vbucket, retryDelay);
            MILLISECONDS.sleep(retryDelay.toMillis());
          }
        }

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);

      } catch (Exception e) {
        LOGGER.debug("Failed to save checkpoint for vbucket {}", vbucket);

        // Remember the exception and throw it later. In the meantime,
        // save as many other checkpoints as possible.
        if (deferredException == null) {
          deferredException = new RuntimeException(e);
        }
      }
    }

    if (deferredException != null) {
      throw deferredException;
    }
  }

  private void upsertXattrs(String documentId, Map<String, Object> content) {
    collection.mutateIn(documentId, singletonList(MutateInSpec.upsert(XATTR_NAME, content).xattr()));
  }

  private void createDocument(String documentId, Map<String, Object> content) {
    try {
      upsertXattrs(documentId, content);
    } catch (DocumentNotFoundException e) {
      collection.upsert(documentId, EMPTY_BYTE_ARRAY, upsertOptions()
          .transcoder(RawBinaryTranscoder.INSTANCE));
      upsertXattrs(documentId, content);
    }
  }

  private String documentIdForVbucket(int vbucket) {
    return checkpointDocumentKeys[vbucket];
  }

  @Override
  public void clear(String bucketUuid, Set<Integer> vbuckets) {
    for (int vbucket : vbuckets) {
      try {
        collection.remove(documentIdForVbucket(vbucket));
      } catch (DocumentNotFoundException alreadyGone) {
        // that's okay
      }
    }
  }

  @Override
  public Map<Integer, Checkpoint> loadExisting(String bucketUuid, Set<Integer> vbuckets) {
    final Map<Integer, Checkpoint> result = new LinkedHashMap<>();

    for (int vbucket : vbuckets) {
      final JsonNode content = readCheckpointForPartition(vbucket);
      if (content != null) {
        final Checkpoint checkpoint = mapper.convertValue(content.get("checkpoint"), Checkpoint.class);
        result.put(vbucket, checkpoint);
      }
    }

    return result;
  }

  private static final String XATTR_NAME = "cbes";

  /**
   * Returns the partition's checkpoint as JSON,
   * or null if the document or XATTR does not exist.
   *
   * @throws RuntimeException if any other error occurs
   */
  private @Nullable JsonNode readCheckpointForPartition(int partition) {
    try {
      String documentId = documentIdForVbucket(partition);
      LookupInResult lookup = collection.lookupIn(documentId, singletonList(LookupInSpec.get(XATTR_NAME).xattr()));
      return mapper.readTree(lookup.contentAs(0, byte[].class));

    } catch (DocumentNotFoundException | PathNotFoundException e) {
      return null;

    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize checkpoint JSON", e);
    }
  }
}
