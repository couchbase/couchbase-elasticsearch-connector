package org.elasticsearch.transport.couchbase.capi;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.couchbase.CouchbaseCAPIService;

/**
 * Use regular expression for parent indexing - assuming define a named group parent as part of regex.
 * Two configurations are relevant for parent mapping:
 * couchbase.documentTypesParentRegex - specify the parent regular expression selector. to select group name 'parent'
 * couchbase.documentTypesParentFormat - optionally specify parent document id format
 * for example:
 * couchbase.documentTypesParentRegex.typeA: ^typeA::(?<parent>.+)
 * couchbase.documentTypesParentFormat.typeA: parentType::<parent>
 *
 * When indexing typeA (typeA::123) document the parent is taken from typeA (123) document Id and composed into
 * parent id according to couchbase.documentTypesParentRegex.typeA configuration. Therefore parent id that is indexed is
 * parentType::123
 *
 * @author tal.maayani on 1/22/2015.
 */
public class RegexParentSelector implements ParentSelector {
    public static final String PARENT = "parent";
    protected Logger logger = Loggers.getLogger(getClass());
    private Map<String, Pattern> documentTypeParentRegexMap;
    private Map<String, String> documentTypeParentFormatMap;

    @Override
    public void configure(Settings settings) {
        Map<String, String> documentTypeParentRegexMap = CouchbaseCAPIService.Config.DOCUMENT_TYPE_PARENT_REGEX.get(settings).getAsMap();
        Map<String, String> documentTypeParentFormatInternalMap = CouchbaseCAPIService.Config.DOCUMENT_TYPE_PARENT_FIELDS.get(settings).getAsMap();
        this.documentTypeParentRegexMap = new HashMap<>();
        this.documentTypeParentFormatMap = new HashMap<>();
        for (String key : documentTypeParentRegexMap.keySet()) {
            String pattern = documentTypeParentRegexMap.get(key);
            this.documentTypeParentRegexMap.put(key, Pattern.compile(pattern));
            logger.info("Using regex {} to select parent for type {}", pattern, key);
            if (documentTypeParentFormatInternalMap.containsKey(key)) {
                String parentFormat = documentTypeParentFormatInternalMap.get(key);
                logger.info("Using parent format {} to select parent of type {}",parentFormat,key);
                documentTypeParentFormatMap.put(key, parentFormat.replace("<parent>","%s"));
            }
        }
    }

    @Override
    public Object getParent(Map<String, Object> doc, String docId, String type) {
        if (documentTypeParentRegexMap.isEmpty()) {
            return null;
        }
        Pattern typePattern = documentTypeParentRegexMap.get(type);
        if (typePattern == null) {
            logger.trace("No parent regex found for type {}", type);
            return null;
        }
        Matcher matcher = typePattern.matcher(docId);
        if (matcher.matches()) {
            String parent = matcher.group(PARENT);
            if (documentTypeParentFormatMap.containsKey(type)) {
                parent = String.format(documentTypeParentFormatMap.get(type),parent);
            }
            return parent;
        }
        return null;
    }
}
