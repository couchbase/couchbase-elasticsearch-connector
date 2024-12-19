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

package com.couchbase.connector.elasticsearch.sink;

import com.fasterxml.jackson.databind.JsonNode;
import org.jspecify.annotations.Nullable;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface SinkTestOps extends Closeable {
  Optional<JsonNode> getDocument(String index, String id, @Nullable String routing);

  long countDocuments(String index);

  List<MultiGetItem> multiGet(String index, Collection<String> ids);

  class MultiGetItem {
    public String id;
    public @Nullable String error;
    public @Nullable JsonNode document;

    public MultiGetItem(String id, @Nullable String error, @Nullable JsonNode document) {
      this.id = requireNonNull(id);
      this.error = error;
      this.document = document;
    }
  }
}
