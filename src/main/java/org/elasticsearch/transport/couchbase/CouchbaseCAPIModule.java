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
