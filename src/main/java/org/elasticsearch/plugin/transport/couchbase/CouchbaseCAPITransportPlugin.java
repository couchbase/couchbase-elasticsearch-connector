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

import java.util.Collection;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
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
    public String name() {
        return "transport-couchbase";
    }

    @Override
    public String description() {
        return "Couchbase Transport";
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

    public void onModule(SettingsModule settingsModule) {
        registerSettingIfMissing(settingsModule, CouchbaseCAPIService.Config.ENABLED);
        registerSettingIfMissing(settingsModule, CouchbaseCAPIService.Config.USERNAME);
        registerSettingIfMissing(settingsModule, CouchbaseCAPIService.Config.PASSWORD);
        registerSettingIfMissing(settingsModule, CouchbaseCAPIService.Config.IGNORE_FAILURES);
        registerSettingIfMissing(settingsModule, CouchbaseCAPIService.Config.IGNORE_DELETES);
        registerSettingIfMissing(settingsModule, CouchbaseCAPIService.Config.NUM_VBUCKETS);
        registerSettingIfMissing(settingsModule, CouchbaseCAPIService.Config.PORT);
        registerSettingIfMissing(settingsModule, CouchbaseCAPIService.Config.BUCKET_UUID_CACHE_EVICT_MS);
        registerSettingIfMissing(settingsModule, CouchbaseCAPIService.Config.COUCHBASE);
    }

    private void registerSettingIfMissing(SettingsModule settingsModule, Setting<?> setting) {
        if (settingsModule.exists(setting) == false) {
            settingsModule.registerSetting(setting);
        }
    }
}
