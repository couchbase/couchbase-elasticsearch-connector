package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.settings.Settings;

public class DefaultKeyFilter implements KeyFilter {

    // The default filter type is exclude, meaning you have to specify which patterns to exclude
    // Specifying no filters should let all documents through
    public static final String DEFAULT_KEY_FILTER_TYPE = "exclude";

    @Override
    public void configure(Settings settings) {
        // Nothing to do here
    }

    @Override
    public Boolean shouldAllow(String index, String docId) {
        return true;
    }
}
