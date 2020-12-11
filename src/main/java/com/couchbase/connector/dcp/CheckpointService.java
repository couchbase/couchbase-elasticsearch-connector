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
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

public class CheckpointService {
  private static final Logger LOGGER = LoggerFactory.getLogger(CheckpointService.class);

  private volatile AtomicReferenceArray<Checkpoint> positions;
  private final CheckpointDao streamPositionDao;
  private final String bucketUuid;
  private final Counter failures = Metrics.counter("save.state.fail", "Failed to save a replication checkpoint document to Couchbase.");
  private volatile boolean dirty;
  private volatile ImmutableList<Long> backfillTargetSeqnos;
  private volatile long remainingBackfillItems = -1;
  private volatile long totalBackfillItems = -1;

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
  public void init(List<Long> backfillTargetSeqnos, Supplier<Map<Integer, Long>> highSeqnoProvider) {
    final int numPartitions = backfillTargetSeqnos.size();
    this.backfillTargetSeqnos = ImmutableList.copyOf(backfillTargetSeqnos);
    this.highSeqnoProvider = requireNonNull(highSeqnoProvider);

    LOGGER.info("Initializing checkpoint service with backfill target seqnos: {}", backfillTargetSeqnos);
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

    this.totalBackfillItems = remainingBackfillItems();
    registerBackfillMetrics();

    return result;
  }

  private long remainingBackfillItems() {
    final List<Long> remainingByPartition = new ArrayList<>(backfillTargetSeqnos.size());

    for (int vbucket = 0, max = backfillTargetSeqnos.size(); vbucket < max; vbucket++) {
      final long backfillTarget = backfillTargetSeqnos.get(vbucket);
      final Checkpoint current = defaultIfNull(positions.get(vbucket), Checkpoint.ZERO);

      // seqno values are unsigned, but subtraction is safe on unsigned values
      final long remainingForPartition = Math.max(0, backfillTarget - current.getSeqno());
      remainingByPartition.add(remainingForPartition);
    }

    LOGGER.info("Remaining backfill by vbucket: {}", remainingByPartition);
    return remainingByPartition.stream()
        .mapToLong(s -> s).sum();
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

  private void registerBackfillMetrics() {
    // deprecated
    Metrics.gauge("backfill", null, this, me -> me.totalBackfillItems);

    Metrics.cachedGauge("backlog",
        "Estimated Couchbase changes yet to be processed by this node.",
        this, CheckpointService::getLocalBacklog);

    Supplier<Long> backfillRemainingSupplier = Suppliers.memoizeWithExpiration(() -> {
      updateBackfillProgress();
      return remainingBackfillItems;
    }, 1, TimeUnit.SECONDS);
    // deprecated
    Metrics.gauge("backfill.remaining", null, this, ignored -> backfillRemainingSupplier.get());

    // deprecated
    Metrics.cachedGauge("backfill.est.time.left", null, this, value -> {
      final long remainingItems = backfillRemainingSupplier.get();
      final long remainingNanos = (long) (remainingItems * Metrics.indexTimePerDocument().mean(NANOSECONDS));
      return NANOSECONDS.toSeconds(remainingNanos);
    });
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

  private void updateBackfillProgress() {
    if (LOGGER.isDebugEnabled()) {
      final List<Long> currentSeqnos = new ArrayList<>();
      for (int i = 0; i < positions.length(); i++) {
        final Checkpoint checkpoint = defaultIfNull(positions.get(i), Checkpoint.ZERO);
        currentSeqnos.add(checkpoint.getSeqno());
      }
      LOGGER.debug("current seqnos: {}", currentSeqnos);
    }

    remainingBackfillItems = remainingBackfillItems();
  }
}
