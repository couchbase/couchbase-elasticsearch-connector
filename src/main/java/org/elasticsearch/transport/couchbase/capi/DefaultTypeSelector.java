package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.couchbase.CouchbaseCAPIService;

public class DefaultTypeSelector implements TypeSelector {

    protected String defaultDocumentType;
    protected String checkpointDocumentType;

    @Override
    public void configure(Settings settings) {
        this.defaultDocumentType = CouchbaseCAPIService.Config.DEFAULT_DOCUMENT_TYPE.get(settings);
        this.checkpointDocumentType = CouchbaseCAPIService.Config.CHECKPOINT_DOCUMENT_TYPE.get(settings);
    }

    @Override
    public String getType(String index, String docId) {
        if (docId.startsWith("_local/")) {
            return this.checkpointDocumentType;
        }
        return this.defaultDocumentType;
    }

}
