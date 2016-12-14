package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.settings.Settings;

public class DefaultKeyFilter implements KeyFilter {

    @Override
    public void configure(Settings settings) {
        // Nothing to do here
    }

    @Override
    public Boolean shouldAllow(String index, String docId) {
        return true;
    }
}
