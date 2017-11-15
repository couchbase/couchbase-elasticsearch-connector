package org.elasticsearch.transport.couchbase.capi;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.couchbase.CouchbaseCAPIService;

/**
 * A delimiter-based type selector for document IDs formatted as "<type><delimiter><suffix>".
 * The delimiter defaults to ":", as in "user:123" would be indexed under the type "user".
 */
public class DelimiterTypeSelector extends DefaultTypeSelector {
    private String documentTypeDelimiter;
    protected Logger logger = Loggers.getLogger(getClass());

    @Override
    public void configure(Settings settings) {
        super.configure(settings);

        this.documentTypeDelimiter = CouchbaseCAPIService.Config.DOCUMENT_TYPE_DELIMITER.get(settings);
        logger.info("Couchbase transport is using type selector with delimiter: {}", documentTypeDelimiter);
    }

    @Override
    public String getType(final String index, final String docId) {
        final int pos = docId.indexOf(documentTypeDelimiter);

        return pos > 0 ? docId.substring(0, pos) : super.getType(index, docId);
    }
}
