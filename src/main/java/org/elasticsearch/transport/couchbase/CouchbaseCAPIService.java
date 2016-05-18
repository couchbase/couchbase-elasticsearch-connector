/**
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package org.elasticsearch.transport.couchbase;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;


public interface CouchbaseCAPIService {

    final class Config {

        // Credentials
        public static final Setting<String> USERNAME = new Setting<>("couchbase.username", s -> "Administrator", Function.identity(), Property.NodeScope);
        public static final Setting<String> PASSWORD = Setting.simpleString("couchbase.password", Property.NodeScope, Property.Filtered);
        public static final Setting<Boolean> ENABLED = Setting.boolSetting("couchbase.enabled", true, Property.NodeScope);

        // Network
        public static final Setting<String> PORT = new Setting<>("couchbase.port", s -> "9091-10091", Function.identity(), Property.NodeScope);

        // Behavior
        public static final Setting<Boolean> IGNORE_FAILURES = Setting.boolSetting("couchbase.ignoreFailures", false, Property.NodeScope);
        public static final Setting<List<String>> IGNORE_DELETES = Setting.listSetting("couchbase.ignoreDeletes", Collections.emptyList(), Function.identity(), Property.NodeScope);

        // Internal
        public static final Setting<Long> BUCKET_UUID_CACHE_EVICT_MS = Setting.longSetting("couchbase.bucketUUIDCacheEvictMs", 300000L, 1000L, Property.NodeScope);
        public static final Setting<Integer> NUM_VBUCKETS = Setting.intSetting("couchbase.num_vbuckets", 1024, Property.NodeScope);

        public static final Setting<Settings> COUCHBASE = Setting.groupSetting("couchbase.", Setting.Property.NodeScope);
    }
}