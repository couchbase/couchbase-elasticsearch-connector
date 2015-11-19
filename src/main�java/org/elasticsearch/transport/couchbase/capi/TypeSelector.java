package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.settings.Settings;

public interface TypeSelector {
    void configure(Settings settings);
    String getType(String index, String docId);
}
