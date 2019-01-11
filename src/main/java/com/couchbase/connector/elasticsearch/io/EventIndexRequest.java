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

import com.couchbase.connector.dcp.Event;
import com.google.common.base.Strings;
import org.elasticsearch.action.index.IndexRequest;

import static java.util.Objects.requireNonNull;

public class EventIndexRequest extends IndexRequest implements EventDocWriteRequest<IndexRequest> {
  private final Event event;

  public EventIndexRequest(String index, String type, String routing, Event event) {
    super(index, type, event.getKey());
    if (Strings.isNullOrEmpty(routing) == false)
      super.routing(routing);
    this.event = requireNonNull(event);
  }

  @Override
  public Event getEvent() {
    return event;
  }

  @Override
  public int estimatedSizeInBytes() {
    return REQUEST_OVERHEAD + source().length();
  }
}
