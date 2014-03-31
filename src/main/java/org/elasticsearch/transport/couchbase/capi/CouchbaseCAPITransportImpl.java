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
package org.elasticsearch.transport.couchbase.capi;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.transport.couchbase.CouchbaseCAPITransport;

import com.couchbase.capi.CAPIBehavior;
import com.couchbase.capi.CAPIServer;
import com.couchbase.capi.CouchbaseBehavior;

public class CouchbaseCAPITransportImpl extends AbstractLifecycleComponent<CouchbaseCAPITransport> implements CouchbaseCAPITransport {

    public static final String DEFAULT_DOCUMENT_TYPE_DOCUMENT = "couchbaseDocument";
    public static final String DEFAULT_DOCUMENT_TYPE_CHECKPOINT = "couchbaseCheckpoint";

    private CAPIBehavior capiBehavior;
    private CouchbaseBehavior couchbaseBehavior;
    private CAPIServer server;
    private Client client;
    private final NetworkService networkService;
    private final IndicesService indicesService;
    private final MetaDataMappingService metaDataMappingService;

    private final String port;
    private final String bindHost;
    private final String publishHost;

    private final String username;
    private final String password;

    private final Boolean resolveConflicts;

    private BoundTransportAddress boundAddress;

    private String defaultDocumentType;
    private String checkpointDocumentType;
    private String dynamicTypePath;

    private final int numVbuckets;

    private final long maxConcurrentRequests;

    @Inject
    public CouchbaseCAPITransportImpl(Settings settings, RestController restController, NetworkService networkService, IndicesService indicesService, MetaDataMappingService metaDataMappingService, Client client) {
        super(settings);
        this.networkService = networkService;
        this.indicesService = indicesService;
        this.metaDataMappingService = metaDataMappingService;
        this.client = client;
        this.port = settings.get("couchbase.port", "9091-10091");
        this.bindHost = componentSettings.get("bind_host");
        this.publishHost = componentSettings.get("publish_host");
        this.username = settings.get("couchbase.username", "Administrator");
        this.password = settings.get("couchbase.password", "");
        this.defaultDocumentType = settings.get("couchbase.defaultDocumentType", DEFAULT_DOCUMENT_TYPE_DOCUMENT);
        this.checkpointDocumentType = settings.get("couchbase.checkpointDocumentType", DEFAULT_DOCUMENT_TYPE_CHECKPOINT);
        this.dynamicTypePath = settings.get("couchbase.dynamicTypePath");
        this.resolveConflicts = settings.getAsBoolean("couchbase.resolveConflicts", true);
        this.maxConcurrentRequests = settings.getAsLong("couchbase.maxConcurrentRequests", 1024L);

        int defaultNumVbuckets = 1024;
        if(System.getProperty("os.name").toLowerCase().contains("mac")) {
            logger.info("Detected platform is Mac, changing default num_vbuckets to 64");
            defaultNumVbuckets = 64;
        }

        this.numVbuckets = settings.getAsInt("couchbase.num_vbuckets", defaultNumVbuckets);
    }

    @Override
    protected void doStart() throws ElasticsearchException {

        // Bind and start to accept incoming connections.
        InetAddress hostAddressX;
        try {
            hostAddressX = networkService.resolveBindHostAddress(bindHost);
        } catch (IOException e) {
            throw new BindHttpException("Failed to resolve host [" + bindHost + "]", e);
        }
        final InetAddress hostAddress = hostAddressX;


        InetAddress publishAddressHostX;
        try {
            publishAddressHostX = networkService.resolvePublishHostAddress(publishHost);
        } catch (IOException e) {
            throw new BindHttpException("FAiled to resolve publish address host [" + publishHost + "]", e);
        }
        final InetAddress publishAddressHost = publishAddressHostX;


        capiBehavior = new ElasticSearchCAPIBehavior(client, logger, defaultDocumentType, checkpointDocumentType, dynamicTypePath, resolveConflicts.booleanValue(), maxConcurrentRequests);
        couchbaseBehavior = new ElasticSearchCouchbaseBehavior(client);

        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
        boolean success = portsRange.iterate(new PortsRange.PortCallback() {
            @Override
            public boolean onPortNumber(int portNumber) {
                try {

                    server = new CAPIServer(capiBehavior, couchbaseBehavior,
                            new InetSocketAddress(hostAddress, portNumber),
                            CouchbaseCAPITransportImpl.this.username,
                            CouchbaseCAPITransportImpl.this.password,
                            numVbuckets);


                    if (publishAddressHost != null) {
                        server.setPublishAddress(publishAddressHost);
                    }

                    server.start();
                } catch (Exception e) {
                    lastException.set(e);
                    return false;
                }
                return true;
            }
        });
        if (!success) {
            throw new BindHttpException("Failed to bind to [" + port + "]",
                    lastException.get());
        }

        InetSocketAddress boundAddress = server.getBindAddress();
        InetSocketAddress publishAddress = new InetSocketAddress(publishAddressHost, boundAddress.getPort());
        this.boundAddress = new BoundTransportAddress(new InetSocketTransportAddress(boundAddress), new InetSocketTransportAddress(publishAddress));
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        if(server != null) {
            try {
                server.stop();
            } catch (Exception e) {
                throw new ElasticsearchException("Error stopping jetty", e);
            }
        }
    }

    @Override
    protected void doClose() throws ElasticsearchException {

    }

    @Override
    public BoundTransportAddress boundAddress() {
        return boundAddress;
    }

}
