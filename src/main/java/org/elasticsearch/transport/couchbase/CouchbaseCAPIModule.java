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

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.transport.couchbase.capi.CouchbaseCAPITransportImpl;

public class CouchbaseCAPIModule extends AbstractModule {

	// pkg private so it is settable by tests
    static Class<? extends CouchbaseCAPITransport> couchbaseCAPITransportImpl = CouchbaseCAPITransportImpl.class;

    public static Class<? extends CouchbaseCAPITransport> getS3ServiceImpl() {
        return couchbaseCAPITransportImpl;
    }

    @Override
    protected void configure() {
    	bind(CouchbaseCAPITransport.class).to(couchbaseCAPITransportImpl).asEagerSingleton();
        bind(CouchbaseCAPI.class).asEagerSingleton();
    }

}
