package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.settings.Settings;

import java.util.Map;

public interface ParentSelector {
    void configure(Settings settings);
    Object getParent(Map<String, Object> doc, String docId, String type);
}
