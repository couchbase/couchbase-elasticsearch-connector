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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

public class CouchbaseCAPI extends AbstractLifecycleComponent {

    private final CouchbaseCAPITransport transport;

    @Inject
    public CouchbaseCAPI(Settings settings, CouchbaseCAPITransport transport) {
        super(settings);
        this.transport = transport;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        transport.start();
        if (logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        transport.stop();
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        transport.close();
    }

}
