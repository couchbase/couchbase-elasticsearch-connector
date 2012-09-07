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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.transport.BindTransportException;
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

    private BoundTransportAddress boundAddress;

    private String defaultDocumentType;
    private String checkpointDocumentType;
    private String dynamicTypePath;

    @Inject
    public CouchbaseCAPITransportImpl(Settings settings, RestController restController, NetworkService networkService, IndicesService indicesService, MetaDataMappingService metaDataMappingService, Client client) {
        super(settings);
        this.networkService = networkService;
        this.indicesService = indicesService;
        this.metaDataMappingService = metaDataMappingService;
        this.client = client;
        this.port = componentSettings.get("port", settings.get("couchbase.port", "9091"));
        this.bindHost = componentSettings.get("bind_host");
        this.publishHost = componentSettings.get("publish_host");
        this.username = settings.get("couchbase.username", "Administrator");
        this.password = settings.get("couchbase.password", "");
        this.defaultDocumentType = settings.get("couchbase.defaultDocumentType", DEFAULT_DOCUMENT_TYPE_DOCUMENT);
        this.checkpointDocumentType = settings.get("couchbase.checkpointDocumentType", DEFAULT_DOCUMENT_TYPE_CHECKPOINT);
        this.dynamicTypePath = settings.get("couchbase.dynamicTypePath");
    }

    @Override
    protected void doStart() throws ElasticSearchException {

        // Bind and start to accept incoming connections.
        InetAddress hostAddressX;
        try {
            hostAddressX = networkService.resolveBindHostAddress(bindHost);
        } catch (IOException e) {
            throw new BindHttpException("Failed to resolve host [" + bindHost + "]", e);
        }
        final InetAddress hostAddress = hostAddressX;


        InetSocketAddress bindAddress = new InetSocketAddress(hostAddress, Integer.parseInt(port));

        InetSocketAddress publishAddress = null;
        String publishAddressString = null;
        try {
            InetAddress publishAddressHost = networkService.resolvePublishHostAddress(publishHost);
            publishAddress = new InetSocketAddress(publishAddressHost, bindAddress.getPort());
            publishAddressString = publishAddressHost.toString();
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }

        capiBehavior = new ElasticSearchCAPIBehavior(client, logger, defaultDocumentType, checkpointDocumentType, dynamicTypePath);
        couchbaseBehavior = new ElasticSearchCouchbaseBehavior(client);
        server = new CAPIServer(capiBehavior, couchbaseBehavior, bindAddress, this.username, this.password);
        if(publishAddressString != null) {
            server.setPublishAddress(publishAddressString);
        }

        try {
            server.start();
        } catch (Exception e) {
            throw new ElasticSearchException("Error starting jetty", e);
        }

        this.boundAddress = new BoundTransportAddress(new InetSocketTransportAddress(bindAddress), new InetSocketTransportAddress(publishAddress));
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        if(server != null) {
            try {
                server.stop();
            } catch (Exception e) {
                throw new ElasticSearchException("Error stopping jetty", e);
            }
        }
    }

    @Override
    protected void doClose() throws ElasticSearchException {

    }

    @Override
    public BoundTransportAddress boundAddress() {
        return boundAddress;
    }

}
