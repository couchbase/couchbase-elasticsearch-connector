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
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;

/**
 * An Elasticsearch request with an attached DCP event.
 * Streamlines bulk request retry handling, simpler than using the
 * {@link BulkRequest#payloads()} mechanism.
 */
public interface EventDocWriteRequest<T> extends DocWriteRequest<T> {
  /**
   * Estimate of the overhead associated with each item in a bulk request.
   */
  int REQUEST_OVERHEAD = 50;

  Event getEvent();

  /**
   * Returns {@link #REQUEST_OVERHEAD} plus the size of the request content.
   */
  int estimatedSizeInBytes();
}
