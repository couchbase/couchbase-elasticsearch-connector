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

import com.couchbase.connector.config.es.DocStructureConfig;
import com.couchbase.connector.config.es.RejectLogConfig;
import com.couchbase.connector.config.es.TypeConfig;
import com.couchbase.connector.dcp.Event;
import com.couchbase.connector.elasticsearch.DocumentLifecycle;
import com.couchbase.connector.elasticsearch.Metrics;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.filter.FilteringParserDelegate;
import com.fasterxml.jackson.core.filter.JsonPointerBasedFilter;
import io.micrometer.core.instrument.Timer;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.common.Nullable;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static com.couchbase.connector.dcp.DcpHelper.isMetadata;
import static java.util.Objects.requireNonNull;

public class RequestFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(RequestFactory.class);

  private static final Timer newIndexRequestTimer = Metrics.timer("new.index.req", "Time spent preparing an Elasticsearch indexing request.");
  private static final JsonFactory factory = new JsonFactory();

  private final DocumentTransformer documentTransformer;

  private final List<TypeConfig> types;
  private final RejectLogConfig rejectLogConfig;

  public RequestFactory(List<TypeConfig> types, DocStructureConfig docStructureConfig, RejectLogConfig rejectLogConfig) {
    this.types = requireNonNull(types);
    this.documentTransformer = new DefaultDocumentTransformer(docStructureConfig);
    this.rejectLogConfig = rejectLogConfig;
  }

  @Nullable
  public EventRejectionIndexRequest newRejectionLogRequest(final EventDocWriteRequest origRequest, BulkItemResponse.Failure f) {
    if (rejectLogConfig.index() == null) {
      origRequest.getEvent().release();
      return null;
    }
    return new EventRejectionIndexRequest(rejectLogConfig.index(), rejectLogConfig.typeName(), origRequest, f);
  }

  @Nullable
  public EventRejectionIndexRequest newRejectionLogRequest(final Event origEvent, MatchResult matchResult, Throwable failure) {
    if (rejectLogConfig.index() == null) {
      origEvent.release();
      return null;
    }
    final DocWriteRequest.OpType opType = origEvent.isMutation() ? DocWriteRequest.OpType.INDEX : DocWriteRequest.OpType.DELETE;
    return new EventRejectionIndexRequest(rejectLogConfig.index(), rejectLogConfig.typeName(),
        origEvent, matchResult.index(), matchResult.typeConfig().type(), opType, failure.getMessage());
  }

  @Nullable
  public EventDocWriteRequest newDocWriteRequest(final Event e) {
    if (isMetadata(e)) {
      return null;
    }

    // Want to hear some good news? Elasticsearch document IDs are limited to 512 bytes,
    // but Couchbase IDs are never longer than 250 bytes.

    final MatchResult matchResult = match(e);
    if (matchResult == null) {
      DocumentLifecycle.logSkippedBecauseMatchedNoRules(e);
      return null; // skip it!
    }
    if (matchResult.typeConfig().ignore()) {
      DocumentLifecycle.logSkippedBecauseMatchedIgnoredType(e, matchResult.typeConfig());
      return null; // skip it!
    }

    DocumentLifecycle.logMatchedTypeRule(e, matchResult.index(), matchResult.typeConfig());

    if (e.isMutation()) {
      return newIndexRequest(e, matchResult);
    }

    if (matchResult.typeConfig().ignoreDeletes()) {
      DocumentLifecycle.logSkippedBecauseRuleSaysIgnoreDeletes(e);
      return null; // skip it!
    }

    return newDeleteRequest(e, matchResult);
  }

  @Nullable
  private EventDeleteRequest newDeleteRequest(final Event event, final MatchResult matchResult) {
    return new EventDeleteRequest(matchResult.index(), matchResult.typeConfig().type(), event);
  }

  @Nullable
  private EventIndexRequest newIndexRequest(final Event event, final MatchResult matchResult) {
    try {
      final Timer.Sample timerContext = Timer.start();
      EventIndexRequest request = new EventIndexRequest(matchResult.index(), matchResult.typeConfig().type(), event);
      request.setPipeline(matchResult.typeConfig().pipeline());
      request.routing(getRouting(event, matchResult.typeConfig().routing()));
      documentTransformer.setSourceFromEventContent(request, event);

      timerContext.stop(newIndexRequestTimer);
      return request.source() == null ? null : request;

    } catch (Exception failure) {
      LOGGER.warn("Failed to create doc write request for {} ; adding an entry to the rejection log instead.", redactUser(event), failure);
      Metrics.rejectionCounter().increment();
      return newRejectionLogRequest(event, matchResult, failure);
    }
  }

  private String getRouting(Event event, JsonPointer routingPointer) throws IOException {
    requireNonNull(event);
    if (routingPointer == null) {
      return null;
    }

    final JsonParser parser = new FilteringParserDelegate(
        factory.createParser(event.getContent()), new JsonPointerBasedFilter(routingPointer), false, false);

    if (parser.nextToken() == null) {
      LOGGER.warn("Document '{}' has no field matching routing JSON pointer '{}'",
          redactUser(event.getKey()), routingPointer);
      return null;
    }

    final String routingValue = parser.getValueAsString();
    if (routingValue == null) {
      LOGGER.warn("Document '{}' has a null or non-scalar value for routing JSON pointer '{}'",
          redactUser(event.getKey()), routingPointer);
      return null;
    }

    LOGGER.trace("Routing value for {} is {}", event.getKey(), routingValue);
    return routingValue;
  }

  @Value.Immutable
  public interface MatchResult {
    TypeConfig typeConfig();

    String index();
  }

  @Nullable // null means no match
  private MatchResult match(final Event event) {
    for (TypeConfig type : types) {
      String index = type.matcher().getIndexIfMatches(event);
      if (index != null) {
        return ImmutableMatchResult.builder()
            .typeConfig(type)
            .index(index)
            .build();
      }
    }
    return null;
  }
}
