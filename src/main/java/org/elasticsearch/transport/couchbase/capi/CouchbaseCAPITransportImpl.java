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
import org.elasticsearch.index.Index;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.indices.IndicesLifecycle.Listener;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.transport.BindTransportException;
import org.elasticsearch.transport.couchbase.CouchbaseCAPITransport;

import com.couchbase.capi.CAPIBehavior;
import com.couchbase.capi.CAPIServer;
import com.couchbase.capi.CouchbaseBehavior;

public class CouchbaseCAPITransportImpl extends AbstractLifecycleComponent<CouchbaseCAPITransport> implements CouchbaseCAPITransport {

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

    private BoundTransportAddress boundAddress;

    @Inject
    public CouchbaseCAPITransportImpl(Settings settings, RestController restController, NetworkService networkService, IndicesService indicesService, MetaDataMappingService metaDataMappingService, Client client) {
        super(settings);
        this.networkService = networkService;
        this.indicesService = indicesService;
        this.metaDataMappingService = metaDataMappingService;
        this.client = client;
        this.port = componentSettings.get("port", settings.get("couchbase.port", "8091"));
        this.bindHost = componentSettings.get("bind_host");
        this.publishHost = componentSettings.get("publish_host");
    }

    @Override
    protected void doStart() throws ElasticSearchException {

        //see if we can register a listener for new indexes
        indicesService.indicesLifecycle().addListener(new Listener() {



            @Override
            public void afterIndexCreated(IndexService indexService) {
                final Index theIndex = indexService.index();

                logger.debug("I see index created event {}", theIndex.getName());

                IndexSettingsService indexSettingsService = indexService.settingsService();
//                indexSettingsService.addListener(new IndexSettingsService.Listener() {
//
//                    @Override
//                    public void onRefreshSettings(Settings changedSettings) {
//                        logger.debug("told to refresh settings");
//                        String documentTypeField = changedSettings.get("couchbase.document_type_field");
//
//                        logger.debug("refreshed index {} has config setting couchbase.document_type_field value {}", theIndex.getName(), documentTypeField);
//                    }
//                });
                Settings indexSettings = indexSettingsService.getSettings();
                String documentTypeField = indexSettings.get("couchbase.document_type_field");
                if(documentTypeField != null) {
                    logger.debug("index {} has config setting couchbase.document_type_field value {}", theIndex.getName(), documentTypeField);
                }

            }

        });

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

        capiBehavior = new ElasticSearchCAPIBehavior(client, logger);
        couchbaseBehavior = new ElasticSearchCouchbaseBehavior(client);
        server = new CAPIServer(capiBehavior, couchbaseBehavior, bindAddress);
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
