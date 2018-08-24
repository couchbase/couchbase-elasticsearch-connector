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

import com.codahale.metrics.Timer;
import com.couchbase.connector.config.es.DocStructureConfig;
import com.couchbase.connector.config.es.RejectLogConfig;
import com.couchbase.connector.config.es.TypeConfig;
import com.couchbase.connector.dcp.Event;
import com.couchbase.connector.elasticsearch.Metrics;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.common.Nullable;

import java.io.IOException;
import java.util.List;

import static com.couchbase.connector.dcp.DcpHelper.isMetadata;
import static java.util.Objects.requireNonNull;

public class RequestFactory {
  private static final Timer newIndexRequestTimer = Metrics.timer("newIndexReq");

  private final DocumentTransformer documentTransformer;

  private final List<TypeConfig> types;
  private final RejectLogConfig rejectLogConfig;

  public RequestFactory(List<TypeConfig> types, DocStructureConfig docStructureConfig, RejectLogConfig rejectLogConfig) {
    this.types = requireNonNull(types);
    this.documentTransformer = new DefaultDocumentTransformer(docStructureConfig);
    this.rejectLogConfig = rejectLogConfig;
  }

  @Nullable
  public EventRejectionIndexRequest newRejectionLogRequest(final EventDocWriteRequest origRequest, BulkItemResponse.Failure f) throws IOException {
    if (rejectLogConfig.index() == null) {
      origRequest.getEvent().release();
      return null;
    }
    return new EventRejectionIndexRequest(rejectLogConfig.index(), rejectLogConfig.typeName(), origRequest, f);
  }

  @Nullable
  public EventDocWriteRequest newDocWriteRequest(final Event e) throws IOException {
    if (isMetadata(e)) {
      return null;
    }

    // Want to hear some good news? Elasticsearch document IDs are limited to 512 bytes,
    // but Couchbase IDs are never longer than 250 bytes.

    final TypeConfig type = getType(e);
    if (type == null || type.ignore()) {
      return null; // skip it!
    }
    if (e.isMutation()) {
      return newIndexRequest(e, type);
    }
    return type.ignoreDeletes() ? null : newDeleteRequest(e, type);
  }

  @Nullable
  private EventDeleteRequest newDeleteRequest(final Event event, final TypeConfig type) {
    return new EventDeleteRequest(type.index(), type.type(), event);
  }

  @Nullable
  private EventIndexRequest newIndexRequest(final Event event, final TypeConfig type) throws IOException {
    final Timer.Context timerContext = newIndexRequestTimer.time();
    EventIndexRequest request = new EventIndexRequest(type.index(), type.type(), event);
    request.setPipeline(type.pipeline());
    documentTransformer.setSourceFromEventContent(request, event);

    timerContext.stop();
    return request.source() == null ? null : request;
  }

  private TypeConfig getType(final Event event) {
    for (TypeConfig type : types) {
      if (type.matcher().test(event)) {
        return type;
      }
    }
    return null;
  }
}
