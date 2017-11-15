package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

/**
 * A delimiter-based type selector for document IDs formatted as "<type><delimiter><suffix>".
 * The delimiter defaults to ":", as in "user:123" would be indexed under the type "user".
 */
public class DelimiterTypeSelector extends DefaultTypeSelector {
    public static final String DEFAULT_DOCUMENT_TYPE_DELIMITER = ":";
    private String documentTypeDelimiter;
    protected ESLogger logger = Loggers.getLogger(getClass());

    public DelimiterTypeSelector() {
        this.documentTypeDelimiter = DEFAULT_DOCUMENT_TYPE_DELIMITER; // Sanity
    }

    @Override
    public void configure(Settings settings) {
        super.configure(settings);

        this.documentTypeDelimiter = settings.get("couchbase.typeSelector.documentTypeDelimiter", DelimiterTypeSelector.DEFAULT_DOCUMENT_TYPE_DELIMITER);
        logger.info("Couchbase transport is using type selector with delimiter: {}", documentTypeDelimiter);
    }

    @Override
    public String getType(final String index, final String docId) {
        final int pos = docId.indexOf(documentTypeDelimiter);

        return pos > 0 ? docId.substring(0, pos) : super.getType(index, docId);
    }
}
