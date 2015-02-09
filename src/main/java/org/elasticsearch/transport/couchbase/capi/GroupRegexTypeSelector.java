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
 * couchbase.documentTypesRegex: ^(?<type>\w+)::.+$
 * @author tal.maayani on 1/23/2015.
 */
public class GroupRegexTypeSelector implements TypeSelector {
    protected ESLogger logger = Loggers.getLogger(getClass());

    private static final String TYPE = "type";
    private String defaultDocumentType;
    private Pattern documentTypesRegex;

    @Override
    public void configure(Settings settings) {
        this.defaultDocumentType = settings.get("couchbase.defaultDocumentType", DefaultTypeSelector.DEFAULT_DOCUMENT_TYPE_DOCUMENT);
        String documentTypesPattern = settings.get("couchbase.documentTypesRegex");
        if (null == documentTypesPattern) {
            logger.error("No configuration found for couchbase.documentTypesRegex, please set types regex");
            throw new RuntimeException("No configuration found for couchbase.documentTypesRegex, please set types regex");
        }
        documentTypesRegex = Pattern.compile(documentTypesPattern);
    }

    @Override
    public String getType(String index, String docId) {
        Matcher matcher = documentTypesRegex.matcher(docId);
        if (matcher.matches()) {
            return matcher.group(TYPE);
        }
        logger.warn("Document Id {} does not match type group regex - use default document type",docId);
        return defaultDocumentType;
    }
}
