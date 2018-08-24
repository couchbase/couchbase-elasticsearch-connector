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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Map;

public class EventRejectionIndexRequest extends EventIndexRequest {
  public EventRejectionIndexRequest(String index, String type, EventDocWriteRequest origRequest, BulkItemResponse.Failure failure) {
    super(index, type, origRequest.getEvent());

    final Map<String, Object> content = ImmutableMap.of(
        "index", origRequest.index(),
        "type", origRequest.type(),
        "action", origRequest.opType(),
        "error", failure.getMessage());
    source(content, XContentType.JSON);
  }
}
