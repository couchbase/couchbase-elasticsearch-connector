/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.connector.elasticsearch;

import com.couchbase.connector.elasticsearch.sink.DeleteOperation;
import com.couchbase.connector.elasticsearch.sink.IndexOperation;
import com.couchbase.connector.elasticsearch.sink.SinkBulkRequestBuilder;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;

import java.util.ArrayList;
import java.util.List;

public class OpenSearchBulkRequestBuilder implements SinkBulkRequestBuilder {
  private final List<BulkOperation> operations = new ArrayList<>();

  public BulkRequest build(String timeout) {
    return new BulkRequest.Builder()
        .operations(operations)
        .timeout(time -> time.time(timeout))
        .build();
  }

  @Override
  public void add(DeleteOperation operation) {
    operations.add(
        new BulkOperation.Builder()
            .delete(op -> op
                .index(operation.getIndex())
                .id(operation.getEvent().getKey())
            )
            .build()
    );
  }

  @Override
  public void add(IndexOperation operation) {
    operations.add(
        new BulkOperation.Builder()
            .index(op -> op
                .index(operation.getIndex())
                .id(operation.getEvent().getKey())
                .document(operation.document())
                .pipeline(operation.pipeline())
                .routing(operation.routing())
            )
            .build()
    );
  }
}
