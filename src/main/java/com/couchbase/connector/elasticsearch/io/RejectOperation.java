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

package com.couchbase.connector.elasticsearch.io;

import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.couchbase.connector.dcp.Event;
import com.google.common.collect.ImmutableMap;

import static java.util.Objects.requireNonNull;

public class RejectOperation extends IndexOperation {

  public RejectOperation(String index, Operation origRequest, BulkResponseItem failure) {
    this(index,
        origRequest.getEvent(),
        origRequest.getIndex(),
        origRequest.type(),
        requireNonNull(failure.error(), "bulk response item did not fail").toString());
  }

  public RejectOperation(String index, Event event, String eventIndex, Operation.Type action, String failureMessage) {
    super(index,
        event,
        ImmutableMap.of(
            "index", eventIndex,
            //"type", eventType,
            "action", action,
            "error", failureMessage),
        null,
        null
    );
  }

  @Override
  public int estimatedSizeInBytes() {
    int wildGuess = 64;
    return REQUEST_OVERHEAD + wildGuess;
  }
}
