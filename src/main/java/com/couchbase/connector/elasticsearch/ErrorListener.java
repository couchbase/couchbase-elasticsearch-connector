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

package com.couchbase.connector.elasticsearch;

import com.couchbase.connector.dcp.Event;
import org.elasticsearch.action.bulk.BulkItemResponse;

public interface ErrorListener {

  void onFailureToCreateIndexRequest(Event event, Throwable error);

  void onError(Event event, Throwable error);

  void onFailedIndexResponse(Event event, BulkItemResponse response);


  ErrorListener NOOP = new ErrorListener() {
    @Override
    public void onFailureToCreateIndexRequest(Event event, Throwable error) {

    }

    @Override
    public void onFailedIndexResponse(Event event, BulkItemResponse response) {

    }

    @Override
    public void onError(Event event, Throwable error) {

    }
  };
}
