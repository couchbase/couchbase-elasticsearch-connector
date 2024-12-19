/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connector.elasticsearch.sink;

import com.couchbase.connector.dcp.Event;
import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

public class IndexOperation extends BaseOperation {
  private final @Nullable String pipeline;
  private final @Nullable String routing;
  private final Object document;

  public IndexOperation(String index, Event event, Object document, @Nullable String pipeline, @Nullable String routing) {
    super(index, event);
    this.pipeline = pipeline;
    this.routing = routing;
    this.document = requireNonNull(document);
  }

  @Override
  public int estimatedSizeInBytes() {
    return REQUEST_OVERHEAD + getEvent().getContent().length;
  }

  @Override
  public void addTo(SinkBulkRequestBuilder bulkRequestBuilder) {
    bulkRequestBuilder.add(this);
  }

  @Override
  public Type type() {
    return Type.INDEX;
  }

  public @Nullable String pipeline() {
    return pipeline;
  }

  public @Nullable String routing() {
    return routing;
  }

  public Object document() {
    return document;
  }
}
