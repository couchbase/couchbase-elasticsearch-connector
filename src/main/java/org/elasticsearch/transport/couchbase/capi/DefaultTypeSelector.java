package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.settings.Settings;

public class DefaultTypeSelector implements TypeSelector {

    public static final String DEFAULT_DOCUMENT_TYPE_DOCUMENT = "couchbaseDocument";
    public static final String DEFAULT_DOCUMENT_TYPE_CHECKPOINT = "couchbaseCheckpoint";

    protected String defaultDocumentType;
    protected String checkpointDocumentType;

    @Override
    public void configure(Settings settings) {
        this.defaultDocumentType = settings.get("couchbase.defaultDocumentType", DEFAULT_DOCUMENT_TYPE_DOCUMENT);
        this.checkpointDocumentType = settings.get("couchbase.checkpointDocumentType", DEFAULT_DOCUMENT_TYPE_CHECKPOINT);
    }

    @Override
    public String getType(String index, String docId) {
        if(docId.startsWith("_local/")) {
            return this.checkpointDocumentType;
        }
        return this.defaultDocumentType;
    }
    
    @Override
    public String getId(String index, String docId) {
    	return docId;
    }

}
