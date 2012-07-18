package org.elasticsearch.transport.couchbase;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.rest.RestController;

public class CouchbaseCAPI extends AbstractLifecycleComponent<CouchbaseCAPI> {

    private final CouchbaseCAPITransport transport;

    private final NodeService nodeService;

    private final RestController restController;

    @Inject
    public CouchbaseCAPI(Settings settings, CouchbaseCAPITransport transport,
            RestController restController, NodeService nodeService) {
        super(settings);
        this.transport = transport;
        this.restController = restController;
        this.nodeService = nodeService;
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        transport.start();
        if (logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
        }
        nodeService.putNodeAttribute("couchbase_address", transport.boundAddress().publishAddress().toString());
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        nodeService.removeNodeAttribute("couchbase_address");
        transport.stop();
    }

    @Override
    protected void doClose() throws ElasticSearchException {
        transport.close();
    }

}
