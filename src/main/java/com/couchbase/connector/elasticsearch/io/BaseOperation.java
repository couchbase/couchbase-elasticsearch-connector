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

import com.couchbase.connector.dcp.Event;

import static java.util.Objects.requireNonNull;

public abstract class BaseOperation implements Operation {
  private final String index;
  private final Event event;

  public BaseOperation(String index, Event event) {
    this.index = requireNonNull(index);
    this.event = requireNonNull(event);
  }

  @Override
  public Event getEvent() {
    return event;
  }

  public String getIndex() {
    return index;
  }
}
