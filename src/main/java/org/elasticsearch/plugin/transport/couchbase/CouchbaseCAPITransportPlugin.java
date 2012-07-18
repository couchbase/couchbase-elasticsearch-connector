package org.elasticsearch.plugin.transport.couchbase;

import static org.elasticsearch.common.collect.Lists.newArrayList;

import java.util.Collection;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.transport.couchbase.CouchbaseCAPI;
import org.elasticsearch.transport.couchbase.CouchbaseCAPIModule;

public class CouchbaseCAPITransportPlugin extends AbstractPlugin {

    private final Settings settings;

    public CouchbaseCAPITransportPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String name() {
        return "transport-couchbase-xdcr";
    }

    @Override
    public String description() {
        return "Couchbase Transport (via XDCR)";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = newArrayList();
        if (settings.getAsBoolean("couchbase.enabled", true)) {
            modules.add(CouchbaseCAPIModule.class);
        }
        return modules;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        Collection<Class<? extends LifecycleComponent>> services = newArrayList();
        if (settings.getAsBoolean("couchbase.enabled", true)) {
            services.add(CouchbaseCAPI.class);
        }
        return services;
    }

}
