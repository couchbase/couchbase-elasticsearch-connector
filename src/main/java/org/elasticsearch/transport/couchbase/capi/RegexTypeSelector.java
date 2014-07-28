package org.elasticsearch.transport.couchbase.capi;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

public class RegexTypeSelector implements TypeSelector {

    protected ESLogger logger = Loggers.getLogger(getClass());
    private String defaultDocumentType;
    private Map<String,String> documentTypePatternStrings;
    private Map<String, Pattern> documentTypePatterns;

    @Override
    public void configure(Settings settings) {
        this.defaultDocumentType = settings.get("couchbase.defaultDocumentType", DefaultTypeSelector.DEFAULT_DOCUMENT_TYPE_DOCUMENT);
        this.documentTypePatterns = new HashMap<String,Pattern>();
        this.documentTypePatternStrings = settings.getByPrefix("couchbase.documentTypes.").getAsMap();
        for (String key : documentTypePatternStrings.keySet()) {
            String pattern = documentTypePatternStrings.get(key);
            logger.info("See document type: {} with pattern: {} compiling...", key, pattern);
            documentTypePatterns.put(key, Pattern.compile(pattern));
        }
    }

    @Override
    public String getType(String index, String docId) {
        for(Map.Entry<String,Pattern> typePattern : this.documentTypePatterns.entrySet()) {
            if(typePattern.getValue().matcher(docId).matches()) {
                return typePattern.getKey();
            }
        }
        return defaultDocumentType;
    }

}
