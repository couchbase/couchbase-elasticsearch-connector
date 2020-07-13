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
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.connector.elasticsearch.io.BackoffPolicyBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
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

public class CouchbaseCheckpointDao implements CheckpointDao {
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseCheckpointDao.class);

  private static final ObjectMapper mapper = new ObjectMapper();

  private final Collection collection;
  private final String[] checkpointDocumentKeys; // indexed by partition (vbucket)
  private final boolean xattrs;

  public CouchbaseCheckpointDao(Collection collection, String clusterId) {
    this(collection, clusterId, true);
  }

  public CouchbaseCheckpointDao(Collection collection, String clusterId, boolean xattrs) {
    this.collection = requireNonNull(collection);
    this.xattrs = xattrs;

    final int numPartitions = CouchbaseHelper.getNumPartitions(collection);
    final String keyPrefix = DcpHelper.metadataDocumentIdPrefix() + clusterId + ":checkpoint:";

    // Brute-forcing the key names so they map to the correct partitions is expensive,
    // so make a lookup table.
    this.checkpointDocumentKeys = new String[numPartitions];
    for (int partition = 0; partition < numPartitions; partition++) {
      final String baseKey = keyPrefix + partition;
      checkpointDocumentKeys[partition] = forceKeyToPartition(baseKey, partition, numPartitions).orElse(baseKey);
    }
  }

  @Override
  public void save(String bucketUuid, Map<Integer, Checkpoint> vbucketToCheckpoint) throws IOException {
    IOException deferredException = null;

    final Iterator<TimeValue> retryDelays = new BackoffPolicyBuilder(truncatedExponentialBackoff(
        TimeValue.timeValueMillis(50), TimeValue.timeValueSeconds(5)))
        .fullJitter()
        .timeout(TimeValue.timeValueSeconds(5)).build().iterator();

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
            final TimeValue retryDelay = retryDelays.next();
            LOGGER.info("Temporary failure saving checkpoint for vbucket {}, retying in {}", vbucket, retryDelay);
            MILLISECONDS.sleep(retryDelay.millis());
          }
        }

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new InterruptedIOException(e.getMessage());

      } catch (Exception e) {
        LOGGER.debug("Failed to save checkpoint for vbucket {}", vbucket);

        // Remember the exception and throw it later. In the mean time,
        // save as many other checkpoints as possible.
        final IOException ioException = toIOException(e);
        if (deferredException == null) {
          deferredException = ioException;
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

  private void createDocument(String documentId, Map<String, Object> content) throws JsonProcessingException {
    if (xattrs) {
      try {
        upsertXattrs(documentId, content);
      } catch (DocumentNotFoundException e) {
        collection.upsert(documentId, EMPTY_BYTE_ARRAY, upsertOptions()
            .transcoder(RawBinaryTranscoder.INSTANCE));
        upsertXattrs(documentId, content);
      }
    } else {
      final String json = mapper.writeValueAsString(content);
      collection.upsert(documentId, json, upsertOptions()
          .transcoder(RawJsonTranscoder.INSTANCE));
    }
  }

  private static IOException toIOException(Throwable t) {
    return t instanceof IOException ? (IOException) t : new IOException(t);
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
  public Map<Integer, Checkpoint> load(String bucketUuid, Set<Integer> vbuckets) throws IOException {
    final Map<Integer, Checkpoint> result = new LinkedHashMap<>();

    for (int vbucket : vbuckets) {
      final String id = documentIdForVbucket(vbucket);
      final JsonNode content = readDocument(id);

      if (content == null) {
        result.put(vbucket, Checkpoint.ZERO);
      } else {
//        final String checkpointBucketUuid = document.get("bucketUuid").asText();
//        if (checkpointBucketUuid.equals(bucketUuid)) {
        final Checkpoint checkpoint = mapper.convertValue(content.get("checkpoint"), Checkpoint.class);
        result.put(vbucket, checkpoint);
//        } else {
//          LOGGER.warn("Bucket UUID mismatch");
//          // todo think about how we would get into this state
//          // todo throw IllegalStateException??
//        }
      }
    }

    return result;
  }

  private static final String XATTR_NAME = "cbes";

  private JsonNode readDocument(String documentId) throws IOException {
    if (xattrs) {
      try {
        LookupInResult lookup = collection.lookupIn(documentId, singletonList(LookupInSpec.get(XATTR_NAME).xattr()));
        return mapper.readTree(lookup.contentAsObject(0).toString());

      } catch (Exception e) {
        return null;
      }
    } else {
      JsonObject doc = collection.get(documentId).contentAsObject();
      if (doc == null) {
        return null;
      }
      return mapper.readTree(doc.toString());
    }
  }
}
