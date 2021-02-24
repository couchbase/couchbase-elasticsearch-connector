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

package com.couchbase.connector.elasticsearch.io;

import com.couchbase.connector.config.es.BulkRequestConfig;
import com.couchbase.connector.dcp.Checkpoint;
import com.couchbase.connector.dcp.CheckpointService;
import com.couchbase.connector.dcp.Event;
import com.couchbase.connector.elasticsearch.DocumentLifecycle;
import com.couchbase.connector.elasticsearch.ErrorListener;
import com.couchbase.connector.elasticsearch.Metrics;
import com.couchbase.connector.util.ThrowableHelper;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static com.couchbase.connector.dcp.DcpHelper.isMetadata;
import static com.couchbase.connector.elasticsearch.io.BackoffPolicyBuilder.truncatedExponentialBackoff;
import static com.couchbase.connector.util.ThrowableHelper.propagateCauseIfPossible;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

/**
 * Inspired by the Elasticsearch client's BulkProcessor.
 * Handles retries and connection failures more reliably (famous last words).
 * <p>
 * NOT THREAD SAFE.
 */
public class ElasticsearchWriter implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchWriter.class);

  private final RestHighLevelClient client;
  private final RequestFactory requestFactory;
  private final CheckpointService checkpointService;
  private final ErrorListener errorListener = ErrorListener.NOOP;
  private final long bufferBytesThreshold;
  private final int bufferActionsThreshold;
  private final TimeValue bulkRequestTimeout;

  private static final TimeValue INITIAL_RETRY_DELAY = timeValueMillis(50);
  private static final TimeValue MAX_RETRY_DELAY = timeValueMinutes(5);

  private final BackoffPolicy backoffPolicy =
      truncatedExponentialBackoff(INITIAL_RETRY_DELAY, MAX_RETRY_DELAY)
          .fullJitter()
          //.timeout(timeValueMinutes(5))
          .build();

  @GuardedBy("this")
  private boolean requestInProgress;

  @GuardedBy("this")
  private long requestStartNanos;

  public ElasticsearchWriter(RestHighLevelClient client, CheckpointService checkpointService,
                             RequestFactory requestFactory,
                             BulkRequestConfig bulkConfig) {
    this.client = requireNonNull(client);
    this.checkpointService = requireNonNull(checkpointService);
    this.requestFactory = requireNonNull(requestFactory);
    this.bufferActionsThreshold = bulkConfig.maxActions();
    this.bufferBytesThreshold = bulkConfig.maxBytes().getBytes();
    this.bulkRequestTimeout = requireNonNull(bulkConfig.timeout());
  }

  private final LinkedHashMap<String, EventDocWriteRequest> buffer = new LinkedHashMap<>();
  private int bufferBytes;

  // Map from vbucket to checkpoint of last ignored event.
  private final Map<Integer, Checkpoint> ignoreBuffer = new HashMap<>();

  /**
   * Appends the given event to the write buffer.
   * Must be followed by a call to {@link #flush}.
   * <p>
   * The writer assumes ownership of the event (is responsible for releasing it).
   */
  public void write(Event event) throws InterruptedException {

    // Regarding the order of bulk operations, Elastic Team Member Adrien Grand says:
    // "You can rely on the fact that operations on the same document
    // (same _index, _type and _id) will be in order. However you can't assume
    // anything for documents that have different indices/types/ids."
    //
    // https://discuss.elastic.co/t/order-of--bulk-request-operations/98124/2
    //
    // This *might* mean it's perfectly safe for a document to be modified
    // more than once in the same Elasticsearch batch, but I'm not sure, especially
    // when it comes to retrying individual actions.
    //
    // Let's say a document is first created and then deleted in the same batch.
    // Is it possible for the creation to fail with TOO_MANY_REQUESTS due to a
    // full bulk queue, but for the deletion to succeed? If so, after the creation
    // is successfully retried, Elasticsearch will be in an inconsistent state;
    // the document will exist but it should not.
    //
    // I do not know whether Elasticsearch guarantees that if an action in a bulk request
    // fails with TOO_MANY_REQUESTS, all subsequent actions also fail with that same
    // error code. All the documentation I've seen suggest that items in a bulk request
    // are completely independent. If you learn otherwise, feel free to banish this
    // paranoid code and change the buffer from a Map into a List.
    //
    // Another possibility would be to use the DCP sequence number as an external version.
    // This would prevent earlier versions from overwriting later ones. The only
    // problem is that a rollback would *really* screw things up. A rolled back
    // document would be stuck in the bad state until being modified with a higher
    // seqno than before the rollback. Anyway, let's revisit this if the
    // "one action per-document per-batch" strategy is identified as a bottleneck.

    final EventDocWriteRequest request = requestFactory.newDocWriteRequest(event);
    if (request == null) {
      try {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("Skipping event, no matching type: {}", redactUser(event));
        }

        if (buffer.isEmpty()) {
          // can ignore immediately
          final Checkpoint checkpoint = event.getCheckpoint();
          if (isMetadata(event)) {
            // Avoid cycle where writing the checkpoints triggers another DCP event.
            LOGGER.debug("Ignoring metadata, not updating checkpoint for {}", event);
            checkpointService.setWithoutMarkingDirty(event.getVbucket(), event.getCheckpoint());
          } else {
            LOGGER.debug("Ignoring event, immediately updating checkpoint for {}", event);
            checkpointService.set(event.getVbucket(), checkpoint);
          }
        } else {
          // ignore later after we've completed a bulk request and saved
          ignoreBuffer.put(event.getVbucket(), event.getCheckpoint());
        }
        return;

      } finally {
        event.release();
      }
    }

    bufferBytes += request.estimatedSizeInBytes();

    // Ensure every (documentID, dest index) pair is unique within a batch.
    // Do this *after* skipping unrecognized / ignored events, so that
    // an ignored deletion does not evict a previously buffered mutation.
    final EventDocWriteRequest evicted = buffer.put(event.getKey() + '\0' + request.index(), request);
    if (evicted != null) {
      String qualifiedDocId = event.getKey(true);
      String evictedQualifiedDocId = evicted.getEvent().getKey(true);
      if (!qualifiedDocId.equals(evictedQualifiedDocId)) {
        LOGGER.warn("DOCUMENT ID COLLISION DETECTED:" +
                " Documents '{}' and '{}' are from different collections" +
                " but have the same destination index '{}'.",
            qualifiedDocId, evictedQualifiedDocId, request.index());
      }

      DocumentLifecycle.logSkippedBecauseNewerVersionReceived(evicted.getEvent(), event.getTracingToken());
      bufferBytes -= evicted.estimatedSizeInBytes();
      evicted.getEvent().release();
    }

    if (bufferIsFull()) {
      flush();
    }
  }

  private Checkpoint adjustForIgnoredEvents(int vbucket, Checkpoint checkpoint) {
    final Checkpoint ignored = ignoreBuffer.remove(vbucket);
    if (ignored == null) {
      return checkpoint;
    }

    if (ignored.getVbuuid() != checkpoint.getVbuuid()) {
      // can't compare the seqnos :-/
      LOGGER.debug("vbuuid of ignored event does not match last written event (rollback?); will disregard ignored event when updating checkpoint.");
      return checkpoint;
    }

    if (Long.compareUnsigned(ignored.getSeqno(), checkpoint.getSeqno()) > 0) {
      LOGGER.debug("Adjusting vbucket {} checkpoint {} for ignored events -> {}", vbucket, checkpoint, ignored);
      return ignored;
    }

    return checkpoint;
  }

  private boolean bufferIsFull() {
    return buffer.size() >= bufferActionsThreshold || bufferBytes >= bufferBytesThreshold;
  }

  public void flush() throws InterruptedException {
    if (buffer.isEmpty()) {
      return;
    }

    try {
      synchronized (this) {
        requestInProgress = true;
        requestStartNanos = System.nanoTime();
      }

      final int totalActionCount = buffer.size();
      final int totalEstimatedBytes = bufferBytes;
      LOGGER.debug("Starting bulk request: {} actions for ~{} bytes", totalActionCount, totalEstimatedBytes);

      final long startNanos = System.nanoTime();

      List<EventDocWriteRequest> requests = new ArrayList<>(buffer.values());
      clearBuffer();

      final Iterator<TimeValue> waitIntervals = backoffPolicy.iterator();
      final Map<Integer, EventDocWriteRequest> vbucketToLastEvent = lenientIndex(r -> r.getEvent().getVbucket(), requests);

      int attemptCounter = 1;
      long indexingTookNanos = 0;
      long totalRetryDelayMillis = 0;

      while (true) {
        if (Thread.interrupted()) {
          requests.forEach(r -> r.getEvent().release());
          Thread.currentThread().interrupt();
          return;
        }

        DocumentLifecycle.logEsWriteStarted(requests, attemptCounter);

        if (attemptCounter == 1) {
          LOGGER.debug("Bulk request attempt #{}", attemptCounter++);
        } else {
          LOGGER.info("Bulk request attempt #{}", attemptCounter++);
        }


        final List<EventDocWriteRequest> requestsToRetry = new ArrayList<>(0);
        final BulkRequest bulkRequest = newBulkRequest(requests);
        bulkRequest.timeout(bulkRequestTimeout);

        final RetryReporter retryReporter = RetryReporter.forLogger(LOGGER);

        try {
          final BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
          final long nowNanos = System.nanoTime();
          final BulkItemResponse[] responses = bulkResponse.getItems();

          indexingTookNanos += bulkResponse.getTook().nanos();

          for (int i = 0; i < responses.length; i++) {
            final BulkItemResponse response = responses[i];
            final BulkItemResponse.Failure failure = response.getFailure();
            final EventDocWriteRequest request = requests.get(i);
            final Event e = request.getEvent();

            if (failure == null) {
              updateLatencyMetrics(e, nowNanos);
              DocumentLifecycle.logEsWriteSucceeded(request);
              e.release();
              continue;
            }

            if (isRetryable(failure)) {
              retryReporter.add(e, failure);
              requestsToRetry.add(request);
              DocumentLifecycle.logEsWriteFailedWillRetry(request);
              continue;
            }

            if (request instanceof EventRejectionIndexRequest) {
              // ES rejected the rejection log entry! Total fail.
              LOGGER.error("Failed to index rejection document for event {}; status code: {} {}", redactUser(e), failure.getStatus(), failure.getMessage());
              Metrics.rejectionLogFailureCounter().increment();
              updateLatencyMetrics(e, nowNanos);
              e.release();

            } else {
              LOGGER.warn("Permanent failure to index event {}; status code: {} {}", redactUser(e), failure.getStatus(), failure.getMessage());
              Metrics.rejectionCounter().increment();
              DocumentLifecycle.logEsWriteRejected(request, failure.getStatus().getStatus(), failure.getMessage());

              // don't release event; the request factory assumes ownership
              final EventRejectionIndexRequest rejectionLogRequest = requestFactory.newRejectionLogRequest(request, failure);
              if (rejectionLogRequest != null) {
                requestsToRetry.add(rejectionLogRequest);
              }
            }

            runQuietly("error listener", () -> errorListener.onFailedIndexResponse(e, response));
          }

          Metrics.indexingRetryCounter().increment(requestsToRetry.size());

          requests = requestsToRetry;

        } catch (ElasticsearchStatusException e) {
          if (e.status() == RestStatus.UNAUTHORIZED) {
            LOGGER.warn("Elasticsearch credentials no longer valid.");
            // todo coordinator.awaitNewConfig("Elasticsearch credentials no longer valid.")
          }

          // Anything else probably means the cluster topology is in transition. Retry!
          LOGGER.warn("Bulk request failed with status {}", e.status(), e);

        } catch (IOException e) {
          // Could indicate timeout, connection failure, or maybe something else.
          // In all of these cases, retry the request!
          if (ThrowableHelper.hasCause(e, ConnectException.class)) {
            LOGGER.debug("Elasticsearch connect exception", e);
            LOGGER.warn("Bulk request failed; could not connect to Elasticsearch.");
          } else {
            LOGGER.warn("Bulk request failed", e);
          }

        } catch (RuntimeException e) {
          requests.forEach(r -> r.getEvent().release());

          // If the worker thread was interrupted, someone wants the worker to stop!
          propagateCauseIfPossible(e, InterruptedException.class);

          // Haven't yet encountered any other kind of RuntimeException in testing.
          // todo retry a few times instead of throwing???
          throw e;
        }

        if (requests.isEmpty()) {
          // EXIT!
          for (Map.Entry<Integer, EventDocWriteRequest> entry : vbucketToLastEvent.entrySet()) {
            final int vbucket = entry.getKey();
            Checkpoint checkpoint = entry.getValue().getEvent().getCheckpoint();
            checkpoint = adjustForIgnoredEvents(vbucket, checkpoint);
            checkpointService.set(entry.getKey(), checkpoint);
          }

          // might have some "ignore" checkpoints left over in the buffer if there
          // were no writes for the same vbucket
          for (Map.Entry<Integer, Checkpoint> entry : ignoreBuffer.entrySet()) {
            checkpointService.set(entry.getKey(), entry.getValue());
          }

          Metrics.bytesCounter().increment(totalEstimatedBytes);
          Metrics.indexTimePerDocument().record(indexingTookNanos / totalActionCount, NANOSECONDS);
          if (totalRetryDelayMillis != 0) {
            Metrics.retryDelayTimer().record(totalRetryDelayMillis, MILLISECONDS);
          }

          if (LOGGER.isInfoEnabled()) {
            final long elapsedMillis = NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            final ByteSizeValue prettySize = new ByteSizeValue(totalEstimatedBytes, ByteSizeUnit.BYTES);
            LOGGER.info("Wrote {} actions ~{} in {} ms",
                totalActionCount, prettySize, elapsedMillis);
          }

          return;
        }

        // retry!
        retryReporter.report();
        Metrics.bulkRetriesCounter().increment();
        final TimeValue retryDelay = waitIntervals.next(); // todo check for hasNext? bail out or continue?
        LOGGER.info("Retrying bulk request in {}", retryDelay);
        MILLISECONDS.sleep(retryDelay.millis());
        totalRetryDelayMillis += retryDelay.millis();
      }
    } finally {
      synchronized (this) {
        requestInProgress = false;
      }
    }
  }

  public synchronized long getCurrentRequestNanos() {
    return requestInProgress ? System.nanoTime() - requestStartNanos : 0;
  }

  private static void updateLatencyMetrics(Event e, long nowNanos) {
    final long elapsedNanos = nowNanos - e.getReceivedNanos();
    Metrics.latencyTimer().record(elapsedNanos, NANOSECONDS);
  }

  private void clearBuffer() {
    buffer.clear();
    bufferBytes = 0;
  }

  private static final Set<RestStatus> fatalStatuses = Collections.unmodifiableSet(
      EnumSet.of(
          RestStatus.BAD_REQUEST, // indexing failed due to field mapping issues; not transient
          RestStatus.NOT_FOUND // index does not exist (auto-creation disabled, or deleting from non-existent index)
      ));

  private static boolean isRetryable(BulkItemResponse.Failure f) {
    return !fatalStatuses.contains(f.getStatus());
    // todo Auth failures are also permanent. Need to see how they're surfaced, and decide how to handle.
  }

  private BulkRequest newBulkRequest(Iterable<EventDocWriteRequest> requests) {
    final BulkRequest bulkRequest = new BulkRequest();
    for (EventDocWriteRequest r : requests) {
      bulkRequest.add(r);
    }
    return bulkRequest;
  }

  private static void runQuietly(String description, Runnable r) {
    try {
      r.run();
    } catch (Exception e) {
      LOGGER.warn("Exception in {}", description, e);
    }
  }

  private static <K, V> Map<K, V> lenientIndex(Function<V, K> keyGenerator, Iterable<V> items) {
    final Map<K, V> result = new HashMap<>();
    for (V item : items) {
      result.put(keyGenerator.apply(item), item);
    }
    return result;
  }

  @Override
  public void close() {
    buffer.values().forEach(e -> e.getEvent().release());
  }
}
