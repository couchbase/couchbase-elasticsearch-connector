package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class RegexTypeSelector extends DefaultTypeSelector {

    protected ESLogger logger = Loggers.getLogger(getClass());
    private Map<String, String> documentTypePatternStrings;
    private Map<String, Pattern> documentTypePatterns;

    @Override
    public void configure(Settings settings) {
        super.configure(settings);

        this.documentTypePatterns = new HashMap<String, Pattern>();
        this.documentTypePatternStrings = settings.getByPrefix("couchbase.typeSelector.documentTypesRegex.").getAsMap();
        for (String key : documentTypePatternStrings.keySet()) {
            String pattern = documentTypePatternStrings.get(key);
            logger.info("See document type: {} with pattern: {} compiling...", key, pattern);
            documentTypePatterns.put(key, Pattern.compile(pattern));
        }
    }

    @Override
    public String getType(String index, String docId) {
        for (Map.Entry<String, Pattern> typePattern : this.documentTypePatterns.entrySet()) {
            if (typePattern.getValue().matcher(docId).matches()) {
                return typePattern.getKey();
            }
        }
        return super.getType(index, docId);
    }

}
