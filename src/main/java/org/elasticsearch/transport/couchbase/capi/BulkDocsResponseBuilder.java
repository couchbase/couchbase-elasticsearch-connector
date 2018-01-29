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

package org.elasticsearch.transport.couchbase.capi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BulkDocsResponseBuilder {
    private final List<Map<String, Object>> results = new ArrayList<>();

    public void acknowledge(String itemId, String itemRev) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", itemId);
        map.put("rev", itemRev);
        results.add(map);
    }

    @SuppressWarnings("unchecked")
    public List<Object> build() {
        return (List<Object>) (List) results;
    }
}
