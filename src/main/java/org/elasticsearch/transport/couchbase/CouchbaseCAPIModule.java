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

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.Modules;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.couchbase.capi.CouchbaseCAPITransportModule;

public class CouchbaseCAPIModule extends AbstractModule implements SpawnModules {

    private final Settings settings;

    public CouchbaseCAPIModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        return ImmutableList.of(Modules.createModule(settings.getAsClass("couchbase.type", CouchbaseCAPITransportModule.class, "org.elasticsearch.couchbase.", "CouchbaseCAPITransportModule"), settings));
    }

    @Override
    protected void configure() {
        bind(CouchbaseCAPI.class).asEagerSingleton();
    }

}
