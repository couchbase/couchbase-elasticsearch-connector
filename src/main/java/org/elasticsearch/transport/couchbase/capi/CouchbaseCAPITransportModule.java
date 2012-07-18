package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.transport.couchbase.CouchbaseCAPITransport;

public class CouchbaseCAPITransportModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(CouchbaseCAPITransport.class).to(CouchbaseCAPITransportImpl.class).asEagerSingleton();
    }

}
