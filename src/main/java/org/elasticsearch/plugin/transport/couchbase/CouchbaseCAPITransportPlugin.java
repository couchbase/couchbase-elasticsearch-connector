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
package org.elasticsearch.plugin.transport.couchbase;

import static com.google.common.collect.Lists.newArrayList;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.couchbase.CouchbaseCAPI;
import org.elasticsearch.transport.couchbase.CouchbaseCAPIModule;
import org.elasticsearch.transport.couchbase.CouchbaseCAPIService;

public class CouchbaseCAPITransportPlugin extends Plugin {

    private final Settings settings;

    public CouchbaseCAPITransportPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Collection<Module> nodeModules() {
        Collection<Module> modules = newArrayList();
        if(CouchbaseCAPIService.Config.ENABLED.get(settings)) {
            modules.add(new CouchbaseCAPIModule());
        }
        return modules;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        Collection<Class<? extends LifecycleComponent>> services = newArrayList();
        if(CouchbaseCAPIService.Config.ENABLED.get(settings)) {
            services.add(CouchbaseCAPI.class);
        }
        return services;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            CouchbaseCAPIService.Config.ENABLED,
            CouchbaseCAPIService.Config.USERNAME,
            CouchbaseCAPIService.Config.PASSWORD,
            CouchbaseCAPIService.Config.IGNORE_FAILURES,
            CouchbaseCAPIService.Config.IGNORE_DELETES,
            CouchbaseCAPIService.Config.WRAP_COUNTERS,
            CouchbaseCAPIService.Config.IGNORE_DOT_INDEXES,
            CouchbaseCAPIService.Config.INCLUDE_INDEXES,
            CouchbaseCAPIService.Config.NUM_VBUCKETS,
            CouchbaseCAPIService.Config.MAX_CONCURRENT_REQUESTS,
            CouchbaseCAPIService.Config.BULK_INDEX_RETRIES,
            CouchbaseCAPIService.Config.BULK_INDEX_RETRIES_WAIT_MS,
            CouchbaseCAPIService.Config.PORT,
            CouchbaseCAPIService.Config.BUCKET_UUID_CACHE_EVICT_MS,
            CouchbaseCAPIService.Config.TYPE_SELECTOR,
            CouchbaseCAPIService.Config.DEFAULT_DOCUMENT_TYPE,
            CouchbaseCAPIService.Config.CHECKPOINT_DOCUMENT_TYPE,
            CouchbaseCAPIService.Config.DOCUMENT_TYPE_DELIMITER,
            CouchbaseCAPIService.Config.DOCUMENT_TYPE_REGEX,
            CouchbaseCAPIService.Config.DOCUMENT_TYPE_REGEX_LIST,
            CouchbaseCAPIService.Config.RESOLVE_CONFLICTS,
            CouchbaseCAPIService.Config.DOCUMENT_TYPE_ROUTING_FIELDS,
            CouchbaseCAPIService.Config.PARENT_SELECTOR,
            CouchbaseCAPIService.Config.DOCUMENT_TYPE_PARENT_FIELDS,
            CouchbaseCAPIService.Config.DOCUMENT_TYPE_PARENT_REGEX,
            CouchbaseCAPIService.Config.DOCUMENT_TYPE_PARENT_FORMAT,
            CouchbaseCAPIService.Config.KEY_FILTER,
            CouchbaseCAPIService.Config.KEY_FILTER_TYPE,
            CouchbaseCAPIService.Config.KEY_FILTER_REGEX_LIST);
    }
}
