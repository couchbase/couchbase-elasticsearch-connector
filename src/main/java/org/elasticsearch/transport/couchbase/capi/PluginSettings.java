package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.NoClassSettingsException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.couchbase.CouchbaseCAPIService;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by David Ostrovsky on 11/24/15.
 */
public class PluginSettings {
    private String checkpointDocumentType;

    private Boolean resolveConflicts;
    private Boolean wrapCounters;
    private Boolean ignoreFailures;
    private Boolean ignoreDotIndexes;

    private int maxConcurrentRequests;

    private int bulkIndexRetries;
    private int bulkIndexRetryWaitMs;

    private TypeSelector typeSelector;
    private ParentSelector parentSelector;
    private KeyFilter keyFilter;

    private Map<String, String> documentTypeRoutingFields;

    private List<String> ignoreDeletes;

    private List<String> includeIndexes;

    private int port;

    public PluginSettings(Settings settings) {
        this.setCheckpointDocumentType(CouchbaseCAPIService.Config.CHECKPOINT_DOCUMENT_TYPE.get(settings));
        this.setResolveConflicts(CouchbaseCAPIService.Config.RESOLVE_CONFLICTS.get(settings));
        this.setWrapCounters(CouchbaseCAPIService.Config.WRAP_COUNTERS.get(settings));
        this.setMaxConcurrentRequests(CouchbaseCAPIService.Config.MAX_CONCURRENT_REQUESTS.get(settings));
        this.setBulkIndexRetries(CouchbaseCAPIService.Config.BULK_INDEX_RETRIES.get(settings));
        this.setBulkIndexRetryWaitMs(CouchbaseCAPIService.Config.BULK_INDEX_RETRIES_WAIT_MS.get(settings));
        this.setIgnoreDeletes(CouchbaseCAPIService.Config.IGNORE_DELETES.get(settings));
        this.getIgnoreDeletes().removeAll(Arrays.asList("", null));
        this.setIgnoreFailures(CouchbaseCAPIService.Config.IGNORE_FAILURES.get(settings));
        this.setDocumentTypeRoutingFields(CouchbaseCAPIService.Config.DOCUMENT_TYPE_ROUTING_FIELDS.get(settings).getAsMap());
        this.setIgnoreDotIndexes(CouchbaseCAPIService.Config.IGNORE_DOT_INDEXES.get(settings));
        this.setIncludeIndexes(CouchbaseCAPIService.Config.INCLUDE_INDEXES.get(settings));
        this.getIncludeIndexes().removeAll(Arrays.asList("", null));
        this.setPort(CouchbaseCAPIService.Config.PORT.get(settings));

        TypeSelector typeSelector;
        Class<? extends TypeSelector> typeSelectorClass = this.getAsClass(CouchbaseCAPIService.Config.TYPE_SELECTOR.get(settings), DefaultTypeSelector.class);
        try {
            typeSelector = typeSelectorClass.newInstance();
        } catch (Exception e) {
            throw new ElasticsearchException("Invalid couchbase.typeSelector setting", e);
        }
        typeSelector.configure(settings);
        this.setTypeSelector(typeSelector);

        ParentSelector parentSelector;
        Class<? extends ParentSelector> parentSelectorClass = this.getAsClass(CouchbaseCAPIService.Config.PARENT_SELECTOR.get(settings), DefaultParentSelector.class);
        try {
            parentSelector = parentSelectorClass.newInstance();
        } catch (Exception e) {
            throw new ElasticsearchException("Invalid couchbase.parentSelector setting", e);
        }
        parentSelector.configure(settings);
        this.setParentSelector(parentSelector);

        KeyFilter keyFilter;
        Class<? extends KeyFilter> keyFilterClass = this.getAsClass(CouchbaseCAPIService.Config.KEY_FILTER.get(settings), DefaultKeyFilter.class);
        try {
            keyFilter = keyFilterClass.newInstance();
        } catch (Exception e) {
            throw new ElasticsearchException("Invalid couchbase.keyFilter setting", e);
        }
        keyFilter.configure(settings);
        this.setKeyFilter(keyFilter);

    }

    @SuppressWarnings({"unchecked"})
    private <T> Class<? extends T> getAsClass(String className, Class<? extends T> defaultClazz) {
        String sValue = className;
        if (sValue == null) {
            return defaultClazz;
        }
        try {
            return (Class<? extends T>) this.getClass().getClassLoader().loadClass(sValue);
        } catch (ClassNotFoundException e) {
            throw new NoClassSettingsException("Failed to load class setting ["
                    + className + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public String toString() {
        return "PluginSettings{" +
                "checkpointDocumentType='" + checkpointDocumentType + '\'' +
                ", resolveConflicts=" + resolveConflicts +
                ", wrapCounters=" + wrapCounters +
                ", ignoreFailures=" + ignoreFailures +
                ", ignoreDotIndexes=" + ignoreDotIndexes +
                ", maxConcurrentRequests=" + maxConcurrentRequests +
                ", bulkIndexRetries=" + bulkIndexRetries +
                ", bulkIndexRetryWaitMs=" + bulkIndexRetryWaitMs +
                ", typeSelector=" + typeSelector.getClass().getCanonicalName() +
                ", parentSelector=" + parentSelector .getClass().getCanonicalName()+
                ", keyFilter=" + keyFilter.getClass().getCanonicalName() +
                ", documentTypeRoutingFields=" + documentTypeRoutingFields +
                ", ignoreDeletes=" + ignoreDeletes +
                ", includeIndexes=" + includeIndexes +
                ", port=" + port +
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

    public long getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    public long getBulkIndexRetries() {
        return bulkIndexRetries;
    }

    public void setBulkIndexRetries(int bulkIndexRetries) {
        this.bulkIndexRetries = bulkIndexRetries;
    }

    public long getBulkIndexRetryWaitMs() {
        return bulkIndexRetryWaitMs;
    }

    public void setBulkIndexRetryWaitMs(int bulkIndexRetryWaitMs) {
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

    public void setMaxConcurrentRequests(int maxConcurrentRequests) {
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

    public int getPort() {
        return this.port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
