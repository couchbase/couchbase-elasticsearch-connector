/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connector.elasticsearch;

import com.couchbase.client.core.json.Mapper;
import com.couchbase.connector.config.es.TypeConfig;
import com.couchbase.connector.dcp.DcpHelper;
import com.couchbase.connector.dcp.Event;
import com.couchbase.connector.elasticsearch.io.EventDocWriteRequest;
import com.couchbase.connector.elasticsearch.io.EventRejectionIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.time.temporal.ChronoUnit.MICROS;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Utility methods for logging document lifecycle events.
 */
public class DocumentLifecycle {

  public enum Milestone {
    RECEIVED_FROM_COUCHBASE,

    MATCHED_TYPE_RULE,
    SKIPPED_BECAUSE_MATCHED_NO_RULES,
    SKIPPED_BECAUSE_RULE_SAYS_IGNORE,
    SKIPPED_BECAUSE_RULE_SAYS_IGNORE_DELETES,
    SKIPPED_BECAUSE_NEWER_VERSION_RECEIVED,

    ELASTICSEARCH_WRITE_STARTED,
    ELASTICSEARCH_WRITE_SUCCEEDED,
    ELASTICSEARCH_WRITE_FAILED_WILL_RETRY,
    ELASTICSEARCH_WRITE_PERMANENTLY_REJECTED,
    ;
  }

  private static final Logger log = LoggerFactory.getLogger(DocumentLifecycle.class);

  private DocumentLifecycle() {
    throw new AssertionError("not instantiable");
  }

  public static void logReceivedFromCouchbase(Event event, int worker) {
    if (shouldLog(event)) {
      LinkedHashMap<String, Object> details = new LinkedHashMap<>();
      details.put("revision", event.getChange().getRevision());
      details.put("type", event.isMutation() ? "mutation" : "deletion");
      details.put("partition", event.getVbucket());
      details.put("sequenceNumber", event.getSeqno());
      details.put("assignedToWorker", worker);
      details.put("usSinceCouchbaseChange(might be inaccurate before Couchbase 7)", event.getChange().getTimestamp().until(Instant.now(), MICROS));
      logMilestone(event, Milestone.RECEIVED_FROM_COUCHBASE, details);
    }
  }

  public static void logMatchedTypeRule(Event event, String index, TypeConfig typeConfig) {
    if (shouldLog(event)) {
      LinkedHashMap<String, Object> details = new LinkedHashMap<>();
      details.put("elasticsearchIndex", index);
      details.put("typeConfig", typeConfig.toString()); // toString() because we don't want Jackson to try to serialize it
      logMilestone(event, Milestone.MATCHED_TYPE_RULE, details);
    }
  }

  public static void logSkippedBecauseMatchedNoRules(Event event) {
    if (shouldLog(event)) {
      logMilestone(event, Milestone.SKIPPED_BECAUSE_MATCHED_NO_RULES);
    }
  }

  public static void logSkippedBecauseMatchedIgnoredType(Event event, TypeConfig typeConfig) {
    if (shouldLog(event)) {
      LinkedHashMap<String, Object> details = new LinkedHashMap<>();
      details.put("typeConfig", typeConfig);
      logMilestone(event, Milestone.SKIPPED_BECAUSE_RULE_SAYS_IGNORE, details);
    }
  }

  public static void logSkippedBecauseRuleSaysIgnoreDeletes(Event event) {
    logMilestone(event, Milestone.SKIPPED_BECAUSE_RULE_SAYS_IGNORE_DELETES);
  }

  public static void logSkippedBecauseNewerVersionReceived(Event event) {
    logMilestone(event, Milestone.SKIPPED_BECAUSE_NEWER_VERSION_RECEIVED);
  }

  public static void logEsWriteStarted(List<EventDocWriteRequest> requests, int attemptCounter) {
    if (log.isDebugEnabled()) {
      LinkedHashMap<String, Object> details = new LinkedHashMap<>();
      details.put("attempt", attemptCounter);
      for (EventDocWriteRequest request : requests) {
        logMilestone(request.getEvent(), Milestone.ELASTICSEARCH_WRITE_STARTED, details);
      }
    }
  }

  public static void logEsWriteSucceeded(EventDocWriteRequest request) {
    if (shouldLog(request)) {
      logMilestone(request.getEvent(), Milestone.ELASTICSEARCH_WRITE_SUCCEEDED);
    }
  }

  public static void logEsWriteFailedWillRetry(EventDocWriteRequest request) {
    if (shouldLog(request)) {
      logMilestone(request.getEvent(), Milestone.ELASTICSEARCH_WRITE_FAILED_WILL_RETRY);
    }
  }

  public static void logEsWriteRejected(EventDocWriteRequest request, int httpStatusCode, String message) {
    if (shouldLog(request)) {
      LinkedHashMap<String, Object> details = new LinkedHashMap<>();
      details.put("httpStatusCode", httpStatusCode);
      details.put("rejectionMessage", message);
      logMilestone(request.getEvent(), Milestone.ELASTICSEARCH_WRITE_PERMANENTLY_REJECTED, details);
    }
  }

  private static void logMilestone(Event event, Milestone milestone) {
    logMilestone(event, milestone, emptyMap());
  }

  private static void logMilestone(Event event, Milestone milestone, Map<String, Object> milestoneDetails) {
    if (shouldLog(event)) {
      LinkedHashMap<String, Object> message = new LinkedHashMap<>();
      message.put("milestone", milestone);
      message.put("tracingToken", event.getTracingToken());
      message.put("documentId", event.getKey(true));
      message.putAll(milestoneDetails);
      message.put("usSinceReceipt", NANOSECONDS.toMicros(System.nanoTime() - event.getReceivedNanos()));
      log.debug(Mapper.encodeAsString(message));
    }
  }

  private static boolean shouldLog(Event event) {
    return log.isDebugEnabled() && !DcpHelper.isMetadata(event);
  }

  private static boolean shouldLog(EventDocWriteRequest request) {
    return shouldLog(request.getEvent()) && !(request instanceof EventRejectionIndexRequest);
  }
}
