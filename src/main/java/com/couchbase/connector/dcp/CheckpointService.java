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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.couchbase.connector.elasticsearch.Metrics;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

public class CheckpointService {
  private static final Logger LOGGER = LoggerFactory.getLogger(CheckpointService.class);

  private volatile AtomicReferenceArray<Checkpoint> positions;
  private final CheckpointDao streamPositionDao;
  private final String bucketUuid;
  private final Meter failures = Metrics.meter("saveStateFail");
  private volatile boolean dirty;
  private volatile ImmutableList<Long> backfillTargetSeqnos;
  private volatile long remainingBackfillItems = -1;
  private volatile long totalBackfillItems = -1;

  public CheckpointService(String bucketUuid, CheckpointDao streamPositionDao) {
    this.bucketUuid = bucketUuid;
    this.streamPositionDao = requireNonNull(streamPositionDao);
  }

  public void init(List<Long> backfillTargetSeqnos) {
    final int numPartitions = backfillTargetSeqnos.size();
    this.backfillTargetSeqnos = ImmutableList.copyOf(backfillTargetSeqnos);

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
      failures.mark();
    }
  }

  private void registerBackfillMetrics() {
    Metrics.gauge("backfill", () -> () -> totalBackfillItems);

    // memoize so the backfill per partition isn't logged twice every time the stats are reported
    final Supplier<Long> remainingBackfillItemsSupplier = Suppliers.memoizeWithExpiration(() -> {
      if (remainingBackfillItems != 0) {
        updateBackfillProgress();
      }
      return remainingBackfillItems;
    }, 250, TimeUnit.MILLISECONDS);

    final Gauge backfillRemainingGauge = Metrics.gauge("backfillRemaining", () -> remainingBackfillItemsSupplier::get);

    Metrics.gauge("backfillEstTimeLeft", () -> () -> {
      final long remaining = (long) backfillRemainingGauge.getValue();
      return new TimeValue((long) (remaining * Metrics.indexTimePerDocument().getSnapshot().getMean()), TimeUnit.NANOSECONDS).toString();
    });
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
