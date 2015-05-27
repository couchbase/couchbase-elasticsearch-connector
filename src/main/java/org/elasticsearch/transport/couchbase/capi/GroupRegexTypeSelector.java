package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Use regular expression to create document types dynamically - assuming define a named group "type" as part of regex.
 * Use couchbase.documentTypesRegex for one regular expression configuration for all types.
 * If you would like to use a specific type regex @use{RegexTypeSelector}
 * Example:
 * couchbase.typeSelector.documentTypesRegex: ^(?<type>\w+)::.+$
 * @author tal.maayani on 1/23/2015, David Ostrovsky
 */
public class GroupRegexTypeSelector extends DefaultTypeSelector {
    protected ESLogger logger = Loggers.getLogger(getClass());

    private static final String TYPE = "type";
    private static final String ID = "id";
    private Pattern documentTypesRegex;
    private Pattern documentIdsRegex;

    @Override
    public void configure(Settings settings) {
        super.configure(settings);

        String documentTypesPattern = settings.get("couchbase.typeSelector.documentTypesRegex");
        if (null == documentTypesPattern) {
            logger.error("No configuration found for couchbase.typeSelector.documentTypesRegex, please set types regex");
            throw new RuntimeException("No configuration found for couchbase.typeSelector.documentTypesRegex, please set types regex");
        }
        documentTypesRegex = Pattern.compile(documentTypesPattern);
        
        String documentIdsPattern = settings.get("couchbase.typeSelector.documentIdsRegex");
        if (null == documentIdsPattern) {
            logger.error("No configuration found for couchbase.typeSelector.documentIdsRegex, please set types regex");
            documentIdsRegex = null;
        }else{
        	documentIdsRegex = Pattern.compile(documentTypesPattern);
        }
    }

    @Override
    public String getType(String index, String docId) {
        Matcher matcher = documentTypesRegex.matcher(docId);
        if (matcher.matches()) {
            return matcher.group(TYPE);
        }

        logger.warn("Document Id {} does not match type group regex - use default document type", docId);
        return super.getType(index, docId);
    }
    
    @Override
    public String getId(final String index, final String docId)
    {
    	if (null == documentIdsRegex) {
    		return docId;
    	}
    	Matcher matcher = documentIdsRegex.matcher(docId);
        if (matcher.matches()) {
            return matcher.group(ID);
        }

        logger.warn("Document Id {} does not match type group regex - use default document type", docId);
        return super.getId(index, docId);
    }
}
