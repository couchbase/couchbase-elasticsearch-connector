/*
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
        public static final Setting<Integer> PORT = Setting.intSetting("couchbase.port", 9091, Property.NodeScope);

        // Behavior
        public static final Setting<Boolean> IGNORE_FAILURES = Setting.boolSetting("couchbase.ignoreFailures", false, Property.NodeScope);
        public static final Setting<Boolean> WRAP_COUNTERS = Setting.boolSetting("couchbase.wrapCounters", false, Property.NodeScope);
        public static final Setting<Boolean> IGNORE_DOT_INDEXES = Setting.boolSetting("couchbase.ignoreDotIndexes", true, Property.NodeScope);
        public static final Setting<Boolean> RESOLVE_CONFLICTS = Setting.boolSetting("couchbase.resolveConflicts", true, Property.NodeScope);
        public static final Setting<List<String>> IGNORE_DELETES = Setting.listSetting("couchbase.ignoreDeletes", Collections.emptyList(), Function.identity(), Property.NodeScope);
        public static final Setting<List<String>> INCLUDE_INDEXES = Setting.listSetting("couchbase.includeIndexes", Collections.emptyList(), Function.identity(), Property.NodeScope);

        // Type Selector
        public static final Setting<String> TYPE_SELECTOR = new Setting<>("couchbase.typeSelector", s -> "org.elasticsearch.transport.couchbase.capi.DefaultTypeSelector", Function.identity(), Property.NodeScope);
        public static final Setting<String> DEFAULT_DOCUMENT_TYPE = new Setting<>("couchbase.typeSelector.defaultDocumentType", s -> "couchbaseDocument", Function.identity(), Property.NodeScope);
        public static final Setting<String> CHECKPOINT_DOCUMENT_TYPE = new Setting<>("couchbase.typeSelector.checkpointDocumentType", s -> "couchbaseCheckpoint", Function.identity(), Property.NodeScope);
        public static final Setting<String> DOCUMENT_TYPE_DELIMITER = new Setting<>("couchbase.typeSelector.documentTypeDelimiter", s -> ":", Function.identity(), Property.NodeScope);
        public static final Setting<String> DOCUMENT_TYPE_REGEX = new Setting<>("couchbase.typeSelector.documentTypesRegex", s -> "", Function.identity(), Property.NodeScope);
        public static final Setting<Settings> DOCUMENT_TYPE_REGEX_LIST = Setting.groupSetting("couchbase.typeSelector.documentTypesRegex.", Setting.Property.NodeScope);

        // Routing
        public static final Setting<Settings> DOCUMENT_TYPE_ROUTING_FIELDS = Setting.groupSetting("couchbase.documentTypeRoutingFields.", Setting.Property.NodeScope);
        public static final Setting<String> PARENT_SELECTOR = new Setting<>("couchbase.parentSelector", s -> "org.elasticsearch.transport.couchbase.capi.DefaultParentSelector", Function.identity(), Property.NodeScope);
        public static final Setting<Settings> DOCUMENT_TYPE_PARENT_FIELDS = Setting.groupSetting("couchbase.parentSelector.documentTypeParentFields.", Setting.Property.NodeScope);
        public static final Setting<Settings> DOCUMENT_TYPE_PARENT_REGEX = Setting.groupSetting("couchbase.parentSelector.documentTypesParentRegex.", Setting.Property.NodeScope);
        public static final Setting<Settings> DOCUMENT_TYPE_PARENT_FORMAT = Setting.groupSetting("couchbase.parentSelector.documentTypesParentFormat.", Setting.Property.NodeScope);

        // Filtering
        public static final Setting<String> KEY_FILTER = new Setting<>("couchbase.keyFilter", s -> "org.elasticsearch.transport.couchbase.capi.DefaultKeyFilter", Function.identity(), Property.NodeScope);
        public static final Setting<String> KEY_FILTER_TYPE = new Setting<>("couchbase.keyFilter.type", s -> "exclude", Function.identity(), Property.NodeScope);
        public static final Setting<Settings> KEY_FILTER_REGEX_LIST = Setting.groupSetting("couchbase.keyFilter.keyFiltersRegex.", Setting.Property.NodeScope);

        // Internal
        public static final Setting<Long> BUCKET_UUID_CACHE_EVICT_MS = Setting.longSetting("couchbase.bucketUUIDCacheEvictMs", 300000L, 1000L, Property.NodeScope);
        public static final Setting<Integer> NUM_VBUCKETS = Setting.intSetting("couchbase.num_vbuckets", 1024, Property.NodeScope);
        public static final Setting<Integer> MAX_CONCURRENT_REQUESTS = Setting.intSetting("couchbase.maxConcurrentRequests", 1024, Property.NodeScope);
        public static final Setting<Integer> BULK_INDEX_RETRIES = Setting.intSetting("couchbase.bulkIndexRetries", 10, Property.NodeScope);
        public static final Setting<Integer> BULK_INDEX_RETRIES_WAIT_MS = Setting.intSetting("couchbase.bulkIndexRetryWaitMs", 1000, Property.NodeScope);
    }
}
