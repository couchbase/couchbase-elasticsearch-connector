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

import com.couchbase.capi.CAPIBehavior;
import com.couchbase.capi.CAPIServer;
import com.couchbase.capi.CouchbaseBehavior;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.transport.couchbase.CouchbaseCAPIService;
import org.elasticsearch.transport.couchbase.CouchbaseCAPITransport;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CouchbaseCAPITransportImpl extends AbstractLifecycleComponent implements CouchbaseCAPITransport {

    private CAPIBehavior capiBehavior;
    private CouchbaseBehavior couchbaseBehavior;
    private CAPIServer server;
    private Client client;
    private final NetworkService networkService;

    private final String port;
    private final String bindHost;
    private final String publishHost;

    private BoundTransportAddress boundAddress;
    private final int numVbuckets;

    private final String username;
    private final String password;

    private long bucketUUIDCacheEvictMs;
    private Cache<String, String> bucketUUIDCache;

    private PluginSettings pluginSettings;

    
    @Inject
    public CouchbaseCAPITransportImpl(Settings settings, NetworkService networkService, Client client) {
        super(settings);
        
        this.networkService = networkService;
        this.client = client;
        this.port = CouchbaseCAPIService.Config.PORT.get(settings);
       
        this.bindHost = settings.get("network.bind_host");
        this.publishHost = settings.get("network.publish_host");
        
        this.username = CouchbaseCAPIService.Config.USERNAME.get(settings);
        this.password = CouchbaseCAPIService.Config.PASSWORD.get(settings);
        
        this.bucketUUIDCacheEvictMs = CouchbaseCAPIService.Config.BUCKET_UUID_CACHE_EVICT_MS.get(settings);
        this.bucketUUIDCache = CacheBuilder.newBuilder().expireAfterWrite(this.bucketUUIDCacheEvictMs, TimeUnit.MILLISECONDS).build();

        int defaultNumVbuckets = 1024;
        if(System.getProperty("os.name").toLowerCase().contains("mac")) {
            logger.info("Detected platform is Mac, changing default num_vbuckets to 64");
            defaultNumVbuckets = 64;
        }
        this.numVbuckets = CouchbaseCAPIService.Config.NUM_VBUCKETS.exists(settings) ?
                CouchbaseCAPIService.Config.NUM_VBUCKETS.get(settings) : defaultNumVbuckets;

        pluginSettings = new PluginSettings(settings);

        // Log settings info
        logger.info("Couchbase transport will ignore delete/expiration operations for these buckets: {}", pluginSettings.getIgnoreDeletes());
        logger.info("Couchbase transport will ignore indexing failures and not throw exception to Couchbase: {}", pluginSettings.getIgnoreFailures());
        logger.info("Couchbase transport is using type selector: {}", pluginSettings.getTypeSelector().getClass().getCanonicalName());
        logger.info("Couchbase transport is using parent selector: {}", pluginSettings.getParentSelector().getClass().getCanonicalName());
        logger.info("Couchbase transport is using key filter: {}", pluginSettings.getKeyFilter().getClass().getCanonicalName());
        for (String key: pluginSettings.getDocumentTypeRoutingFields().keySet()) {
            String routingField = pluginSettings.getDocumentTypeRoutingFields().get(key);
            logger.info("Using field {} as routing for type {}", routingField, key);
        }
        logger.info("Plugin Settings: {}", pluginSettings.toString());
    }
    
    private boolean result = false;

    @Override
    protected void doStart() throws ElasticsearchException {
        // Bind and start to accept incoming connections.
        InetAddress[] hostAddressX;
        try {
            hostAddressX = networkService.resolveBindHostAddresses(null);

        } catch (IOException e) {
            throw new BindHttpException("Failed to resolve host [" + bindHost + "]", e);
        }

        InetAddress publishAddressHostX;
        try {
            publishAddressHostX = networkService.resolvePublishHostAddresses(null);
        } catch (IOException e) {
            throw new BindHttpException("Failed to resolve publish address host [" + publishHost + "]", e);
        }
        final InetAddress publishAddressHost = publishAddressHostX;

        logger.info(("Resolved publish host:" + publishAddressHost));

        InetAddress hostAddress;
        if(hostAddressX.length > 0)
            hostAddress = hostAddressX[hostAddressX.length-1];
        else
            hostAddress = publishAddressHostX;
        final InetAddress bindAddress = hostAddress;

        logger.info(("Resolved bind host:" + bindAddress));

        capiBehavior = new ElasticSearchCAPIBehavior(client, logger, bucketUUIDCache, pluginSettings);
        couchbaseBehavior = new ElasticSearchCouchbaseBehavior(client, logger, bucketUUIDCache, pluginSettings);

        PortsRange portsRange = new PortsRange(port);

        logger.info("Using port(s):"+ port);

        final AtomicReference<Exception> lastException = new AtomicReference<>();
     
        boolean success = portsRange.iterate(portNumber -> {
                AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                    try {
                        logger.info("Starting transport-couchbase plugin server on address:" + bindAddress + " port: " + portNumber + " publish address: " + publishAddressHost);
                        server = new CAPIServer(capiBehavior, couchbaseBehavior,
                                new InetSocketAddress(bindAddress, portNumber),
                                CouchbaseCAPITransportImpl.this.username,
                                CouchbaseCAPITransportImpl.this.password,
                                numVbuckets);

                        if (publishAddressHost != null) {
                            server.setPublishAddress(publishAddressHost);
                        }

                        server.start();
                        result = true;
                    } catch (Exception e) {
                        lastException.set(e);
                        result = false;
                    }

                    return null;
                });
                return result;
            });
        if (!success) {
            throw new BindHttpException("Failed to bind to [" + port + "]",
                    lastException.get());
        }

        InetSocketAddress boundAddress = server.getBindAddress();
        InetSocketAddress publishAddress = new InetSocketAddress(publishAddressHost, boundAddress.getPort());
        
        logger.info("Host: {}, Port {}", publishAddressHost.getHostAddress(), boundAddress.getPort());
        
        InetSocketTransportAddress[] array = new InetSocketTransportAddress[1];
        array[0] = new InetSocketTransportAddress(boundAddress);
        
        this.boundAddress = new BoundTransportAddress(array, new InetSocketTransportAddress(publishAddress));
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
