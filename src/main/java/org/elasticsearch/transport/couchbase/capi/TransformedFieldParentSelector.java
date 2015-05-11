package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Select parent based on transformed field value. Usage:
 *
 * <pre>
 * {@code
 * couchbase.parentSelector: org.elasticsearch.transport.couchbase.capi.TransformedFieldParentSelector
 * couchbase.documentTypeParentFields.childType: doc.parentIdField/(.+)/parentType:$1
 * }
 * </pre>
 *
 * @author tungd on 4/26/15.
 */

public class TransformedFieldParentSelector implements ParentSelector {
    protected ESLogger logger = Loggers.getLogger(getClass());

    public static final String FIELD_DELIMITER = "/";

    private Map<String, FieldTransform> documentTypeParentFields;

    private class FieldTransform {
        public String field;
        public Pattern match;
        public String replace;

        public FieldTransform(String field, String match, String replace) {
            this.field = field;
            this.match = Pattern.compile(match);
            this.replace = replace;
        }

        @Override
        public String toString() {
            return match + "/" + replace;
        }

        public Object transform(Object input) {
            if (!(input instanceof String)) {
                return input;
            }

            Matcher matcher = match.matcher((String)input);
            if (!matcher.matches()) {
                return input;
            }

            return matcher.replaceFirst(replace);
        }
    }

    @Override
    public void configure(Settings settings) {
        Map<String, String> config = settings.getByPrefix(
                "couchbase.documentTypeParentFields.").getAsMap();

        documentTypeParentFields = new HashMap<String, FieldTransform>();

        for (String key : config.keySet()) {
            String[] parts = config.get(key).split(FIELD_DELIMITER);

            if (parts.length != 3) {
                logger.error("Invalid transformation defined for type {}: {}", key, config.get(key));
                continue;
            }

            String field = parts[0];
            FieldTransform transformer = new FieldTransform(field, parts[1], parts[2]);
            documentTypeParentFields.put(key, transformer);

            logger.info(
                    "Using field {} as parent for type {}, with transformation: {}",
                    field, key, transformer);
        }
    }

    @Override
    public Object getParent(Map<String, Object> doc, String docId, String type) {
        FieldTransform transform = null;

        if (documentTypeParentFields != null
                && documentTypeParentFields.containsKey(type)) {
            transform = documentTypeParentFields.get(type);
        }
        if (transform == null) {
            return null;
        }

        return transform.transform(ElasticSearchCAPIBehavior.JSONMapPath(doc,
                transform.field));
    }
}
