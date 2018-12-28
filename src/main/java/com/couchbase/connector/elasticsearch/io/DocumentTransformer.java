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
import org.elasticsearch.action.index.IndexRequest;

public interface DocumentTransformer {
  /**
   * Sets the `source` property of the given index request if the
   * given event is eligible for replication to Elasticsearch,
   * otherwise does nothing.
   */
  void setSourceFromEventContent(IndexRequest indexRequest, Event event);
}
