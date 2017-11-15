package org.elasticsearch.transport.couchbase.capi;

import java.util.List;
import java.util.Map;

/**
 * Created by David Ostrovsky on 11/24/15.
 */
public class PluginSettings {
    public static final String DEFAULT_DOCUMENT_TYPE_CHECKPOINT = "couchbaseCheckpoint";

    private String checkpointDocumentType;

    private Boolean resolveConflicts;
    private Boolean wrapCounters;
    private Boolean ignoreFailures;
    private Boolean ignoreDotIndexes;

    private String dynamicTypePath;

    private long maxConcurrentRequests;

    private long bulkIndexRetries;
    private long bulkIndexRetryWaitMs;

    private TypeSelector typeSelector;
    private ParentSelector parentSelector;
    private KeyFilter keyFilter;

    private Map<String, String> documentTypeRoutingFields;

    private List<String> ignoreDeletes;

    private List<String> includeIndexes;

    public PluginSettings() {
        this.setCheckpointDocumentType(DEFAULT_DOCUMENT_TYPE_CHECKPOINT);
        this.setResolveConflicts(true);
        this.setWrapCounters(false);
        this.setIgnoreFailures(false);
        this.setDynamicTypePath("");
        this.setMaxConcurrentRequests(1024);
        this.bulkIndexRetries = 10L;
        this.bulkIndexRetryWaitMs = 1000L;
        this.typeSelector = null;
        this.parentSelector = null;
        this.keyFilter = null;
        this.documentTypeRoutingFields = null;
        this.ignoreDeletes = null;
        this.ignoreDotIndexes = true;
    }

    public PluginSettings(String checkpointDocumentType, Boolean resolveConflicts, Boolean wrapCounters, Boolean ignoreFailures, String dynamicTypePath, long maxConcurrentRequests, long bulkIndexRetries, long bulkIndexRetryWaitMs, TypeSelector typeSelector, ParentSelector parentSelector, KeyFilter keyFilter, Map<String, String> documentTypeRoutingFields, List<String> ignoreDeletes, Boolean ignoreDotIndexes, List<String> includeIndexes) {
        this.includeIndexes = includeIndexes;
        this.setCheckpointDocumentType(checkpointDocumentType);
        this.setResolveConflicts(resolveConflicts);
        this.setWrapCounters(wrapCounters);
        this.setIgnoreFailures(ignoreFailures);
        this.setDynamicTypePath(dynamicTypePath);
        this.setMaxConcurrentRequests(maxConcurrentRequests);
        this.bulkIndexRetries = bulkIndexRetries;
        this.bulkIndexRetryWaitMs = bulkIndexRetryWaitMs;
        this.typeSelector = typeSelector;
        this.parentSelector = parentSelector;
        this.keyFilter = keyFilter;
        this.documentTypeRoutingFields = documentTypeRoutingFields;
        this.ignoreDeletes = ignoreDeletes;
        this.ignoreDotIndexes = ignoreDotIndexes;
    }

    @Override
    public String toString() {
        return "PluginSettings{" +
                "checkpointDocumentType='" + checkpointDocumentType + '\'' +
                ", resolveConflicts=" + resolveConflicts +
                ", wrapCounters=" + wrapCounters +
                ", ignoreFailures=" + ignoreFailures +
                ", ignoreDotIndexes=" + ignoreDotIndexes +
                ", dynamicTypePath='" + dynamicTypePath + '\'' +
                ", maxConcurrentRequests=" + maxConcurrentRequests +
                ", bulkIndexRetries=" + bulkIndexRetries +
                ", bulkIndexRetryWaitMs=" + bulkIndexRetryWaitMs +
                ", typeSelector=" + typeSelector.getClass().getCanonicalName() +
                ", parentSelector=" + parentSelector.getClass().getCanonicalName() +
                ", keyFilter=" + keyFilter.getClass().getCanonicalName() +
                ", documentTypeRoutingFields=" + documentTypeRoutingFields +
                ", ignoreDeletes=" + ignoreDeletes +
                ", includeIndexes=" + includeIndexes +
                '}';
    }

    public String getCheckpointDocumentType() {
        return checkpointDocumentType;
    }

    public void setCheckpointDocumentType(String checkpointDocumentType) {
        this.checkpointDocumentType = checkpointDocumentType;
    }

    public Boolean getResolveConflicts() {
        return resolveConflicts;
    }

    public Boolean getWrapCounters() {
        return wrapCounters;
    }

    public Boolean getIgnoreFailures() {
        return ignoreFailures;
    }

    public String getDynamicTypePath() {
        return dynamicTypePath;
    }

    public void setDynamicTypePath(String dynamicTypePath) {
        this.dynamicTypePath = dynamicTypePath;
    }

    public long getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    public long getBulkIndexRetries() {
        return bulkIndexRetries;
    }

    public void setBulkIndexRetries(long bulkIndexRetries) {
        this.bulkIndexRetries = bulkIndexRetries;
    }

    public long getBulkIndexRetryWaitMs() {
        return bulkIndexRetryWaitMs;
    }

    public void setBulkIndexRetryWaitMs(long bulkIndexRetryWaitMs) {
        this.bulkIndexRetryWaitMs = bulkIndexRetryWaitMs;
    }

    public TypeSelector getTypeSelector() {
        return typeSelector;
    }

    public void setTypeSelector(TypeSelector typeSelector) {
        this.typeSelector = typeSelector;
    }

    public ParentSelector getParentSelector() {
        return parentSelector;
    }

    public void setParentSelector(ParentSelector parentSelector) {
        this.parentSelector = parentSelector;
    }

    public KeyFilter getKeyFilter() {
        return keyFilter;
    }

    public void setKeyFilter(KeyFilter keyFilter) {
        this.keyFilter = keyFilter;
    }

    public Map<String, String> getDocumentTypeRoutingFields() {
        return documentTypeRoutingFields;
    }

    public void setDocumentTypeRoutingFields(Map<String, String> documentTypeRoutingFields) {
        this.documentTypeRoutingFields = documentTypeRoutingFields;
    }

    public List<String> getIgnoreDeletes() {
        return ignoreDeletes;
    }

    public void setIgnoreDeletes(List<String> ignoreDeletes) {
        this.ignoreDeletes = ignoreDeletes;
    }

    public void setResolveConflicts(Boolean resolveConflicts) {
        this.resolveConflicts = resolveConflicts;
    }

    public void setWrapCounters(Boolean wrapCounters) {
        this.wrapCounters = wrapCounters;
    }

    public void setIgnoreFailures(Boolean ignoreFailures) {
        this.ignoreFailures = ignoreFailures;
    }

    public void setMaxConcurrentRequests(long maxConcurrentRequests) {
        this.maxConcurrentRequests = maxConcurrentRequests;
    }

    public Boolean getIgnoreDotIndexes() {
        return ignoreDotIndexes;
    }

    public void setIgnoreDotIndexes(Boolean ignoreDotIndexes) {
        this.ignoreDotIndexes = ignoreDotIndexes;
    }

    public List<String> getIncludeIndexes() {
        return includeIndexes;
    }

    public void setIncludeIndexes(List<String> includeIndexes) {
        this.includeIndexes = includeIndexes;
    }
}
