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

import com.couchbase.connector.elasticsearch.Metrics;
import com.google.common.base.Stopwatch;
import io.micrometer.core.instrument.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class CheckpointService {
  private static final Logger LOGGER = LoggerFactory.getLogger(CheckpointService.class);

  private volatile AtomicReferenceArray<Checkpoint> positions;
  private final CheckpointDao streamPositionDao;
  private final String bucketUuid;
  private final Counter failures = Metrics.counter("save.state.fail", "Failed to save a replication checkpoint document to Couchbase.");
  private volatile boolean dirty;

  // Supplies a map from partition to sequence number for all partitions this
  // connector instance is responsible for.
  private volatile Supplier<Map<Integer, Long>> highSeqnoProvider = Collections::emptyMap;

  public CheckpointService(String bucketUuid, CheckpointDao streamPositionDao) {
    this.bucketUuid = bucketUuid;
    this.streamPositionDao = requireNonNull(streamPositionDao);
  }

  /**
   * @param highSeqnoProvider a supplier to invoke to get the current sequence numbers
   * in all partitions this connector instance is responsible for.
   */
  public void init(int numPartitions, Supplier<Map<Integer, Long>> highSeqnoProvider) {
    this.highSeqnoProvider = requireNonNull(highSeqnoProvider);
    this.positions = new AtomicReferenceArray<>(numPartitions);
  }

  public void set(int vbucket, Checkpoint position) {
    setWithoutMarkingDirty(vbucket, position);
    dirty = true;
  }

  public void setWithoutMarkingDirty(int vbucket, Checkpoint position) {
    LOGGER.debug("New position for vbucket {} is {}", vbucket, position);
    positions.set(vbucket, position);
  }

  public synchronized Map<Integer, Checkpoint> load(Set<Integer> vbuckets) throws IOException {
    final Map<Integer, Checkpoint> result = streamPositionDao.load(bucketUuid, vbuckets);
    LOGGER.debug("Loaded checkpoints: {}", result);

    for (Map.Entry<Integer, Checkpoint> entry : result.entrySet()) {
      final int partition = entry.getKey();
      setWithoutMarkingDirty(partition, entry.getValue());
    }

    registerBacklogMetrics();

    return result;
  }

  public synchronized void save() {
    if (!dirty) {
      LOGGER.debug("Connector state unchanged since last save.");
      return;
    }

    try {
      final Map<Integer, Checkpoint> partitionToPosition = new HashMap<>();
      for (int i = 0; i < positions.length(); i++) {
        partitionToPosition.put(i, positions.get(i));
      }
      streamPositionDao.save(bucketUuid, partitionToPosition);
      dirty = false;
      LOGGER.info("Saved connector state.");

    } catch (Exception t) {
      LOGGER.warn("Failed to save connector state.", t);
      failures.increment();
    }
  }

  private void registerBacklogMetrics() {
    Metrics.cachedGauge("backlog",
        "Estimated Couchbase changes yet to be processed by this node.",
        this, CheckpointService::getLocalBacklog);
  }

  /**
   * Returns an estimate of the total number of unprocessed sequence numbers
   * in all partitions this connector instance is responsible for.
   */
  private long getLocalBacklog() {
    Stopwatch timer = Stopwatch.createStarted();
    Map<Integer, Long> highSeqnos = highSeqnoProvider.get();
    timer.stop();
    LOGGER.info("Getting current seqnos took {}", timer);

    long result = 0;
    for (Map.Entry<Integer, Long> entry : highSeqnos.entrySet()) {
      int partition = entry.getKey();
      long seqno = entry.getValue();

      Checkpoint checkpoint = positions.get(partition);
      if (checkpoint == null) {
        LOGGER.warn("Can't calculate local backlog for partition {}; no checkpoint available (yet?)", partition);
        continue;
      }

      long backlogForPartition = Math.max(0, seqno - checkpoint.getSeqno());
      LOGGER.debug("Local backlog for partition {}: {} (connector: {} server: {})", partition, backlogForPartition, checkpoint.getSeqno(), seqno);
      try {
        result = Math.addExact(result, backlogForPartition);
      } catch (ArithmeticException e) {
        result = Long.MAX_VALUE;
      }

    }
    return result;
  }
}
