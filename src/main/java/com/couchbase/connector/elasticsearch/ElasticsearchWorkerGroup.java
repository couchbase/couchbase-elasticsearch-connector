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

package com.couchbase.connector.elasticsearch;

import com.couchbase.connector.config.es.BulkRequestConfig;
import com.couchbase.connector.dcp.CheckpointService;
import com.couchbase.connector.dcp.Event;
import com.couchbase.connector.elasticsearch.io.ElasticsearchWriter;
import com.couchbase.connector.elasticsearch.io.RequestFactory;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ElasticsearchWorkerGroup implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchWorkerGroup.class);

  private final ImmutableList<ElasticsearchWorker> workers;

  // Workers communicate failures by writing them to this queue
  private final BlockingQueue<Throwable> fatalErrorQueue = new LinkedBlockingQueue<>();

  public ElasticsearchWorkerGroup(ElasticsearchOps client,
                                  CheckpointService checkpointService,
                                  RequestFactory requestFactory,
                                  ErrorListener errorListener,
                                  BulkRequestConfig bulkRequestConfig) {
    checkArgument(bulkRequestConfig.concurrentRequests() > 0, "must have at least one worker");

    final ImmutableList.Builder<ElasticsearchWorker> workersBuilder = ImmutableList.builder();
    for (int i = 0; i < bulkRequestConfig.concurrentRequests(); i++) {
      workersBuilder.add(ElasticsearchWorker.newWorker(
          new ElasticsearchWriter(client, checkpointService, requestFactory, bulkRequestConfig), fatalErrorQueue, errorListener));
    }
    this.workers = workersBuilder.build();
  }

  public void submit(Event e) {
    // Events for the same document ID must always be handled by the same worker.
    final int workerIndex = e.getVbucket() % workers.size();
    DocumentLifecycle.logReceivedFromCouchbase(e, workerIndex);
    workers.get(workerIndex).submit(e);
  }

  public Throwable awaitFatalError() throws InterruptedException {
    // SECONDS.sleep(4);
    //return new RuntimeException("fake failure");
    return fatalErrorQueue.take();
  }

  public long getQueueSize() {
    return workers.stream()
        .mapToLong(ElasticsearchWorker::getQueueSize)
        .sum();
  }

  /**
   * Returns the duration in milliseconds of the active request that started the longest time ago,
   * or zero if there are no active requests.
   */
  public long getCurrentRequestMillis() {
    return NANOSECONDS.toMillis(workers.stream()
        .mapToLong(ElasticsearchWorker::getCurrentRequestNanos)
        .max()
        .orElseThrow(() -> new AssertionError("There should be at least one worker.")));
  }

  @Override
  public void close() {
    final Duration timeout = Duration.ofSeconds(3);
    for (ElasticsearchWorker w : workers) {
      w.close();
    }
    try {
      for (ElasticsearchWorker w : workers) {
        if (!w.join(Math.max(1, timeout.toMillis()))) {
          LOGGER.warn("Worker {} failed to stop after {}", w, timeout);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Interrupted while waiting for workers to stop.");
    }
  }
}
