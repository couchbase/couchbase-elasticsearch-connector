package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    protected ESLogger logger = Loggers.getLogger(getClass());
    private Map<String, Pattern> documentTypeParentRegexMap;
    private Map<String, String> documentTypeParentFormatMap;

    @Override
    public void configure(Settings settings) {
        ImmutableMap<String, String> documentTypeParentRegexMap = settings.getByPrefix("couchbase.documentTypesParentRegex.").getAsMap();
        ImmutableMap<String, String> documentTypeParentFormatInternalMap = settings.getByPrefix("couchbase.documentTypesParentFormat.").getAsMap();
        this.documentTypeParentRegexMap = new HashMap<String, Pattern>();
        this.documentTypeParentFormatMap = new HashMap<String, String>();
        for (String key : documentTypeParentRegexMap.keySet()) {
            String pattern = documentTypeParentRegexMap.get(key);
            this.documentTypeParentRegexMap.put(key, Pattern.compile(pattern));
            logger.info("Using regex {} to select parent for type {}", pattern, key);
            if (documentTypeParentFormatInternalMap.containsKey(key)) {
                String parentFormat = documentTypeParentFormatInternalMap.get(key);
                logger.info("Using parent format {} to select parent of type {}",parentFormat,key);
                documentTypeParentFormatMap.put(key,parentFormat.replace("<parent>","%s"));
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
