package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.settings.Settings;

import java.util.Map;

public interface ParentSelector {
    void configure(Settings settings);

    Object getParent(Map<String, Object> doc, String docId, String type);

    /**
     * Returns the ID of the document's parent if it is embedded in the given child document ID,
     * or {@code null} if the parent ID cannot be determined without access to the child document content.
     * Also returns {@code null} if the given type does not have a parent type;
     * call {@link #typeHasParent(String)} first to disambiguate.
     */
    String getParent(String docId, String type);

    /**
     * Returns true if the the plugin configuration specifies the given document type has a parent.
     */
    boolean typeHasParent(String type);
}
