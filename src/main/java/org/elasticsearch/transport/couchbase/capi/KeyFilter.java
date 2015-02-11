package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.settings.Settings;

public interface KeyFilter {
    void configure(Settings settings);
    Boolean shouldAllow(String index, String docId);
}
