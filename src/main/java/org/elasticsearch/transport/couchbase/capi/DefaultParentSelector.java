package org.elasticsearch.transport.couchbase.capi;

import java.util.Map;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

/**
 * Get parent document Id according to field within document json
 * @author tal.maayani on 1/22/2015.
 */
public class DefaultParentSelector implements ParentSelector {
    protected ESLogger logger = Loggers.getLogger(getClass());

    private Map<String, String> documentTypeParentFields;

    @Override
    public void configure(Settings settings) {
        this.documentTypeParentFields = settings.getByPrefix("couchbase.parentSelector.documentTypeParentFields.").getAsMap();

        for (String key: documentTypeParentFields.keySet()) {
            String parentField = documentTypeParentFields.get(key);
            logger.info("Using field {} as parent for type {}", parentField, key);
        }
    }

    @Override
    public Object getParent(Map<String, Object> doc, String docId, String type) {
        String parentField = null;
        if(documentTypeParentFields != null && documentTypeParentFields.containsKey(type)) {
            parentField = documentTypeParentFields.get(type);
        }
        if (parentField == null) return null;

        return ElasticSearchCAPIBehavior.JSONMapPath(doc, parentField);
    }
}
