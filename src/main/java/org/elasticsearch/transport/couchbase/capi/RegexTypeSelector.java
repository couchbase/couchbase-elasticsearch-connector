package org.elasticsearch.transport.couchbase.capi;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

public class RegexTypeSelector extends DefaultTypeSelector {

    protected ESLogger logger = Loggers.getLogger(getClass());
    private Map<String,String> documentTypePatternStrings;
    private Map<String, Pattern> documentTypePatterns;
    
    private Map<String,String> documentIdPatternStrings;
    private Map<String, Pattern> documentIdPatterns;

    @Override
    public void configure(Settings settings) {
        super.configure(settings);

        this.documentTypePatterns = new HashMap<String,Pattern>();
        this.documentTypePatternStrings = settings.getByPrefix("couchbase.documentTypes.").getAsMap();
        for (String key : documentTypePatternStrings.keySet()) {
            String pattern = documentTypePatternStrings.get(key);
            logger.info("See document type: {} with pattern: {} compiling...", key, pattern);
            documentTypePatterns.put(key, Pattern.compile(pattern));
        }
        
                this.documentIdPatternStrings = settings.getByPrefix("couchbase.documentIds.").getAsMap();	
        if (null == documentIdPatternStrings) {
        	this.documentIdPatterns = null;
        }else{
        	this.documentIdPatterns = new HashMap<String,Pattern>();
        	for (String key : documentIdPatternStrings.keySet()) {
        		String pattern = documentIdPatternStrings.get(key);
       			logger.info("See document id: {} with pattern: {} compiling...", key, pattern);
       			documentIdPatterns.put(key, Pattern.compile(pattern));
       		}
        }
    }

    @Override
    public String getType(String index, String docId) {
        for(Map.Entry<String,Pattern> typePattern : this.documentTypePatterns.entrySet()) {
            if(typePattern.getValue().matcher(docId).matches()) {
                return typePattern.getKey();
            }
        }
        return super.getType(index, docId);
    }
    
    @Override
    public String getId(final String index, final String docId)
    {
    	if (null == documentIdPatterns) {
    		return docId;
    	}
    	for(Map.Entry<String,Pattern> typeId : this.documentIdPatterns.entrySet()) {
             if(typeId.getValue().matcher(docId).matches()) {
                 return typeId.getKey();
             }
        }
        return super.getId(index, docId);
    }
}
