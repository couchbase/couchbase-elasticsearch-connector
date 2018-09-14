/*
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package org.elasticsearch.transport.couchbase.capi;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.json.MetricsModule;
import com.couchbase.capi.CAPIBehavior;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;

import javax.servlet.UnavailableException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.io.BaseEncoding.base64;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.elasticsearch.transport.couchbase.capi.CompatibilityHelper.isCreated;
import static org.elasticsearch.transport.couchbase.capi.CompatibilityHelper.setPipeline;

public class ElasticSearchCAPIBehavior implements CAPIBehavior {

    protected static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.registerModule(new MetricsModule(SECONDS, MILLISECONDS, false));
    }

    private final AtomicLong nextRequestId = new AtomicLong();

    protected Client client;
    protected ESLogger logger;

    protected final MetricRegistry metricRegistry;
    protected final Timer bulkDocsTimer;
    protected final Timer revsDiffTimer;
    protected final Counter activeRevsDiffRequests;
    protected final Counter activeBulkDocsRequests;
    protected final Meter tooManyConcurrentRequestsMeter;
    protected final Timer deletionRoutingTimer;

    protected Cache<String, String> bucketUUIDCache;

    protected PluginSettings pluginSettings;

    public ElasticSearchCAPIBehavior(Client client, ESLogger logger, Cache<String, String> bucketUUIDCache, PluginSettings pluginSettings, MetricRegistry metricRegistry) {
        this.client = client;
        this.logger = logger;
        this.pluginSettings = pluginSettings;
        this.bucketUUIDCache = bucketUUIDCache;
        this.metricRegistry = metricRegistry;

        this.bulkDocsTimer = metricRegistry.timer("bulkDocs");
        this.revsDiffTimer = metricRegistry.timer("revsDiff");
        this.activeRevsDiffRequests = metricRegistry.counter("activeRevsDiffRequests");
        this.activeBulkDocsRequests = metricRegistry.counter("activeBulkDocsRequests");
        this.tooManyConcurrentRequestsMeter = metricRegistry.meter("tooManyConcurrentRequests");
        this.deletionRoutingTimer = metricRegistry.timer("deletionRouting");
    }

    @Override
    public Map<String, Object> welcome() {
        Map<String, Object> responseMap = new HashMap<>();
        responseMap.put("welcome", "elasticsearch-transport-couchbase");
        return responseMap;
    }

    @Override
    public String databaseExists(String database) {
        String index = getElasticSearchIndexNameFromDatabase(database);
        IndicesExistsRequestBuilder existsBuilder = client.admin().indices().prepareExists(index);
        IndicesExistsResponse response = existsBuilder.execute().actionGet();
        if (response.isExists()) {
            String uuid = getBucketUUIDFromDatabase(database);
            if (uuid != null) {
                logger.debug("included uuid, validating");
                String actualUUID = getBucketUUID("default", index);
                if (!uuid.equals(actualUUID)) {
                    return "uuids_dont_match";
                }
            } else {
                logger.debug("no uuid in database name");
            }
            return null;
        }
        return "missing";
    }

    @Override
    public Map<String, Object> getDatabaseDetails(String database) {
        String doesNotExistReason = databaseExists(database);
        if (doesNotExistReason == null) {
            Map<String, Object> responseMap = new HashMap<>();
            responseMap.put("db_name", getDatabaseNameWithoutUUID(database));
            return responseMap;
        }
        return null;
    }

    @Override
    public boolean createDatabase(String database) {
        throw new UnsupportedOperationException("Creating indexes is not supported");
    }

    @Override
    public boolean deleteDatabase(String database) {
        throw new UnsupportedOperationException("Deleting indexes is not supported");
    }

    @Override
    public boolean ensureFullCommit(String database) {
        return true;
    }

    private long nextRequestId() {
        return nextRequestId.incrementAndGet();
    }

    private void throwIfTooManyConcurrentRequests() throws UnavailableException {
        long bulkDocsRequests = activeBulkDocsRequests.getCount();
        long revsDiffRequests = activeRevsDiffRequests.getCount();

        if (bulkDocsRequests + revsDiffRequests >= pluginSettings.getMaxConcurrentRequests()) {
            tooManyConcurrentRequestsMeter.mark();

            logger.error("Too many concurrent requests. _bulk_docs requests: {}, _revs_diff requests: {}, Max configured: {}",
                    bulkDocsRequests, revsDiffRequests, pluginSettings.getMaxConcurrentRequests());

            throw new UnavailableException("Too many concurrent requests");
        }
    }

    @Override
    public Map<String, Object> revsDiff(String database, Map<String, Object> revsMap) throws UnavailableException {
        throwIfTooManyConcurrentRequests();

        final long requestId = nextRequestId();
        final Stopwatch stopwatch = Stopwatch.createStarted();

        activeRevsDiffRequests.inc();
        try {
            logger.debug("enter revsDiff requestId={} database={}", requestId, database);
            logger.trace("_revs_diff request for {} : {}", database, revsMap);

            // start with all entries in the response map
            Map<String, Object> responseMap = new HashMap<>();
            for (Entry<String, Object> entry : revsMap.entrySet()) {
                String id = entry.getKey();
                String revs = (String) entry.getValue();
                Map<String, String> rev = new HashMap<>();
                rev.put("missing", revs);
                responseMap.put(id, rev);
            }
            logger.trace("_revs_diff response for {} is: {}", database, responseMap);

            // if resolve conflicts mode is enabled
            // perform a multi-get query to find information
            // about revisions we already have
            if (pluginSettings.getResolveConflicts()) {
                String index = getElasticSearchIndexNameFromDatabase(database);
                // the following debug code is verbose in the hopes of better understanding CBES-13
                MultiGetResponse response = null;
                if (client != null) {
                    MultiGetRequestBuilder builder = client.prepareMultiGet();
                    if (builder != null) {
                        if (index == null) {
                            logger.debug("index is null");
                        }
                        int added = 0;
                        for (String id : responseMap.keySet()) {
                            String type = pluginSettings.getTypeSelector().getType(index, id);
                            if (hasCustomRouting(type)) {
                                // if this type requires special routing, we can't find it without the doc body
                                // so we skip this id in the lookup to avoid errors
                                continue;
                            }
                            builder = builder.add(index, type, id);
                            added++;
                        }
                        if (builder != null) {
                            if (added > 0) {
                                ActionFuture<MultiGetResponse> laf = builder.execute();
                                if (laf != null) {
                                    response = laf.actionGet();
                                } else {
                                    logger.debug("laf was null");
                                }
                            } else {
                                logger.debug("skipping multiget, no documents to look for");
                            }
                        } else {
                            logger.debug("builder was null 2");
                        }
                    } else {
                        logger.debug("builder was null");
                    }
                } else {
                    logger.debug("client was null");
                }
                if (response != null) {
                    for (MultiGetItemResponse item : response) {
                        if (item.isFailed()) {
                            logger.warn("_revs_diff get failure on index: {} id: {} message: {}", item.getIndex(), item.getId(), item.getFailure().getMessage());
                        } else {
                            if (item.getResponse().isExists()) {
                                String itemId = item.getId();
                                Map<String, Object> source = item.getResponse().getSourceAsMap();
                                if (source != null) {
                                    Map<String, Object> meta = (Map<String, Object>) source.get("meta");
                                    if (meta != null) {
                                        String rev = (String) meta.get("rev");
                                        //retrieve the revision passed in from Couchbase
                                        Map<String, String> sourceRevMap = (Map<String, String>) responseMap.get(itemId);
                                        String sourceRev = sourceRevMap.get("missing");
                                        if (rev.equals(sourceRev)) {
                                            // if our revision is the same as the source rev
                                            // remove it from the response map
                                            responseMap.remove(itemId);
                                            logger.trace("_revs_diff already have id: {} rev: {}", itemId, rev);
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    logger.debug("response was null");
                }
                logger.trace("_revs_diff response AFTER conflict resolution {}", responseMap);
            }

            return responseMap;

        } finally {
            activeRevsDiffRequests.dec();

            stopwatch.stop();
            revsDiffTimer.update(stopwatch.elapsed(NANOSECONDS), NANOSECONDS);
            logger.debug("exit revsDiff requestId={} database={} elapsedMs={}",
                    requestId, database, stopwatch.elapsed(MILLISECONDS));
        }
    }

    @Override
    public List<Object> bulkDocs(String database, List<Map<String, Object>> docs) throws UnavailableException {
        throwIfTooManyConcurrentRequests();

        final long requestId = nextRequestId();
        final Stopwatch stopwatch = Stopwatch.createStarted();

        activeBulkDocsRequests.inc();
        try {
            logger.debug("enter bulkDocs requestId={} database={}", requestId, database);

            final String index = getElasticSearchIndexNameFromDatabase(database);

            //if set to true - all delete operations will be ignored
            //ignoreDeletes contains a list of indexes to be ignored when delete events occur
            //index list can be set in the elasticsearch.yml file using
            //the key: couchbase.ignore.delete  the value is colon separated:  index1:index2:index3
            final boolean ignoreDelete = shouldIgnoreDeletes(index);
            logger.trace("ignoreDelete = {}", ignoreDelete);

            // keep a map of the id - rev for building the response
            final Map<String, String> revisions = new HashMap<>();

            // put requests into this map, not directly into the bulk request
            final Map<String, IndexRequest> bulkIndexRequests = new HashMap<>();
            final Map<String, DeleteRequest> bulkDeleteRequests = new HashMap<>();

            // Handles deletion requests for documents that use non-default routing
            final DeletionRouter deletionRouter = new DeletionRouter(client, index, pluginSettings, deletionRoutingTimer);

            // keeps track of the ID/revision pairs for which we'll acknowledge receipt to satisfy the XDCR mechanism.
            final BulkDocsResponseBuilder responseBuilder = new BulkDocsResponseBuilder();

            logger.trace("Bulk doc entry is {}", docs);
            for (Map<String, Object> doc : docs) {

                // these are the top-level elements that could be in the document sent by Couchbase
                final Map<String, Object> meta = (Map<String, Object>) doc.get("meta");
                Map<String, Object> json = (Map<String, Object>) doc.get("json");
                final String base64 = (String) doc.get("base64");

                if (meta == null) {
                    // if there is no meta-data section, there is nothing we can do
                    logger.warn("Document without meta in bulk_docs, ignoring....");
                    continue;
                }

                final boolean deleted = meta.containsKey("deleted") ? (Boolean) meta.get("deleted") : false;
                final String id = (String) meta.get("id");
                final String rev = (String) meta.get("rev");

                if (id == null) {
                    // if there is no id in the metadata, something is seriously wrong
                    logger.warn("Document metadata does not have an id, ignoring...");
                    continue;
                }

                // Filter documents by ID.
                // Delete operations are always allowed through to ES, to make sure newly configured
                // filters don't cause documents to stay in ES forever.
                if (!pluginSettings.getKeyFilter().shouldAllow(index, id) && !deleted) {
                    // Document ID matches one of the filters, not passing it to on to ES.
                    responseBuilder.acknowledge(id, rev);
                    logger.trace("Document doesn't pass configured key filters, not storing: {}", id);
                    continue;
                }

                if (deleted) {
                    // if this is only a delete anyway, don't bother looking at the body
                    json = new HashMap<>();
                } else if ("non-JSON mode".equals(meta.get("att_reason")) || "invalid_json".equals(meta.get("att_reason"))) {
                    // optimization, this tells us the body isn't json
                    json = new HashMap<>();
                } else if (json == null && base64 != null) {
                    // no plain json, let's try parsing the base64 data
                    try {
                        byte[] decodedData = base64().decode(base64);
                        try {
                            // now try to parse the decoded data as json
                            json = (Map<String, Object>) mapper.readValue(decodedData, Map.class);
                        } catch (IOException e) {
                            json = new HashMap<>();
                            if (pluginSettings.getWrapCounters()) {
                                logger.trace("Trying to parse decoded base64 data as a long and wrap it as a counter document, id: {}", meta.get("id"));
                                try {
                                    long value = Long.parseLong(new String(decodedData));
                                    logger.trace("Parsed data as long: {}", value);
                                    json.put("value", value);
                                } catch (Exception e2) {
                                    logger.error("Unable to parse decoded base64 data as either JSON or long, indexing stub for id: {}", meta.get("id"));
                                    logger.error("Body was: {} Parse error was: {} Long parse error was: {}", new String(decodedData), e, e2);
                                }
                            } else {
                                logger.error("Unable to parse decoded base64 data as JSON, indexing stub for id: {}", meta.get("id"));
                                logger.error("Body was: {} Parse error was: {}", new String(decodedData), e);
                            }
                        }
                    } catch (IllegalArgumentException e) {
                        logger.error("Unable to decoded base64, indexing stub for id: {}", meta.get("id"));
                        logger.error("Base64 was was: {} Parse error was: {}", base64, e);
                        json = new HashMap<>();
                    }
                }

                // at this point we know we have the document meta-data
                // and the document contents to be indexed are in json

                final Map<String, Object> toBeIndexed = new HashMap<>();
                toBeIndexed.put("meta", meta);
                toBeIndexed.put("doc", json);

                revisions.put(id, rev);

                long ttl = 0;
                final Number expiration = (Number) meta.get("expiration"); // Integer or Long
                if (expiration != null && expiration.longValue() > 0) {
                    ttl = (expiration.longValue() * 1000) - System.currentTimeMillis();
                    logger.debug("Document {} has expiration set, which is not supported in ES 5.0+", id);
                }

                final String type = type(index, id);
                final String routingField = routingField(type);
                logger.trace("Selecting type {} for document {} in index {}", type, id, index);

                if (deleted) {
                    if (ignoreDelete) {
                        // Acknowledge each delete request as if ES returned successful delete confirmation.
                        responseBuilder.acknowledge(id, rev);
                    } else {
                        final DeleteRequest deleteRequest = client.prepareDelete(index, type, id).request();
                        bulkDeleteRequests.put(id, deleteRequest);

                        if (hasParent(type) || hasCustomRouting(type)) {
                            // Must specify routing
                            String inferredRouting = inferRoutingFromDocumentId(id, type);
                            if (inferredRouting != null) {
                                deleteRequest.routing(inferredRouting);
                            } else {
                                // Need to do a query to find the correct route.
                                // Remember the doomed IDs and look up the routes later all at once.
                                deletionRouter.addRoutingLater(id);
                            }
                        }
                    }
                } else {
                    final IndexRequestBuilder indexBuilder = client.prepareIndex(index, type, id);
                    indexBuilder.setSource(toBeIndexed);

                    setParent(indexBuilder, toBeIndexed, id, type);
                    setRouting(indexBuilder, toBeIndexed, id, routingField);
                    setPipeline(indexBuilder, pluginSettings);

                    if (!ignoreDelete && ttl > 0) {
                        indexBuilder.setTTL(ttl);
                    }

                    IndexRequest indexRequest = indexBuilder.request();
                    bulkIndexRequests.put(id, indexRequest);
                }
            }

            // Handle deletion requests for documents with non-default routing
            deletionRouter.augmentBulkRequests(bulkIndexRequests, bulkDeleteRequests);

            int attempt = 0;
            BulkResponse response = null;
            long retriesLeft = pluginSettings.getBulkIndexRetries();
            final StringBuilder errors = new StringBuilder();
            BulkRequestBuilder bulkBuilder;

            do {
                // build the bulk request for this iteration
                bulkBuilder = client.prepareBulk();
                for (Entry<String, IndexRequest> entry : bulkIndexRequests.entrySet()) {
                    bulkBuilder.add(entry.getValue());
                }
                for (Entry<String, DeleteRequest> entry : bulkDeleteRequests.entrySet()) {
                    bulkBuilder.add(entry.getValue());
                }

                attempt++;

                if (bulkBuilder.numberOfActions() == 0) {
                    break;
                }

                if (attempt > 1) { // at least second time through
                    try {
                        Thread.sleep(pluginSettings.getBulkIndexRetryWaitMs());
                    } catch (InterruptedException e) {
                        errors.append(e.toString());
                        break;
                    }
                }

                try {
                    response = bulkBuilder.execute().actionGet();
                } catch (Exception e) {
                    logger.error("Failed to execute bulk operation", e);

                    errors.append("Error indexing bulk with document IDs: [");

                    for (Entry<String, IndexRequest> entry : bulkIndexRequests.entrySet()) {
                        errors.append(entry.getKey()).append(", ");
                        if (shouldIgnoreFailures()) {
                            responseBuilder.acknowledge(entry.getKey(), revisions.get(entry.getKey()));
                        }
                    }
                    for (Entry<String, DeleteRequest> entry : bulkDeleteRequests.entrySet()) {
                        errors.append(entry.getKey()).append(", ");
                        if (shouldIgnoreFailures()) {
                            responseBuilder.acknowledge(entry.getKey(), revisions.get(entry.getKey()));
                        }
                    }

                    errors.append("]").append(System.lineSeparator());
                    errors.append("ERROR: ").append(e).append(System.lineSeparator());
                    break; // Do not retry this kind of failure
                }

                if (response != null) {
                    for (BulkItemResponse bulkItemResponse : response.getItems()) {
                        final String itemId = bulkItemResponse.getId();
                        final String itemRev = revisions.get(itemId);

                        if (!bulkItemResponse.isFailed()) {
                            responseBuilder.acknowledge(itemId, itemRev);

                            // remove the item from the bulk requests list so we don't try to index it again
                            bulkIndexRequests.remove(itemId);
                            bulkDeleteRequests.remove(itemId);
                        } else {
                            Failure failure = bulkItemResponse.getFailure();

                            // If the error is fatal, don't retry the request.
                            if (failureMessageAppearsFatal(failure.getMessage())) {
                                logger.error("Error indexing document, will NOT retry. Document id: " + itemId + " exception: " + failure.getMessage());

                                bulkIndexRequests.remove(itemId);
                                bulkDeleteRequests.remove(itemId);

                                if (shouldIgnoreFailures()) {
                                    // pretend the operation succeeded, acknowledge as normal.
                                    responseBuilder.acknowledge(itemId, itemRev);
                                } else {
                                    errors.append(failure.getMessage());
                                    errors.append(System.lineSeparator());
                                }
                            } else {
                                logger.warn("Error indexing document, will retry. Document id: " + itemId + " exception: " + failure.getMessage());
                            }
                        }
                    }
                }
                retriesLeft--;

            } while (response != null && response.hasFailures() && retriesLeft > 0);

            if (response == null && bulkBuilder.numberOfActions() > 0) {
                errors.append("Indexing error: bulk index response was null").append(System.lineSeparator());
            }
            if (retriesLeft == 0) {
                errors.append("Indexing error: bulk index failed after all retries").append(System.lineSeparator());
            }

            if (errors.length() > 0) {
                if (shouldIgnoreFailures()) {
                    logger.error("bulk index requestId={} Ignoring failures: {}", requestId, errors);
                } else {
                    logger.error("bulk index requestId={} Failing due to errors: {}", requestId, errors);
                    throw new RuntimeException(errors.toString());
                }
            } else {
                if (attempt == 1) {
                    logger.debug("Bulk index succeeded on first try");
                } else {
                    logger.warn("Bulk index succeeded after {} tries", attempt);
                }
            }

            return responseBuilder.build();

        } finally {
            activeBulkDocsRequests.dec();

            stopwatch.stop();
            bulkDocsTimer.update(stopwatch.elapsed(NANOSECONDS), NANOSECONDS);
            logger.debug("exit bulkDocs requestId={} database={} elapsedMs={}",
                    requestId, database, stopwatch.elapsed(MILLISECONDS));
        }
    }

    /**
     * Returns true if the specified document type is routed to an ES shard
     * based a field of the document content.
     */
    private boolean hasCustomRouting(String type) {
        return pluginSettings.getDocumentTypeRoutingFields() != null && pluginSettings.getDocumentTypeRoutingFields().containsKey(type);
    }

    /**
     * Returns the ES routing for the document, or {@code null} if the route cannot be inferred from the
     * document ID. A return value of {@code null} does not necessarily mean the document uses default routing.
     */
    private String inferRoutingFromDocumentId(String id, String type) {
        if (hasCustomRouting(type)) {
            return null;
        }
        // If the parent ID is embedded in the child ID, the parent selector can tell us
        // the parent ID, which is used for routing.
        return pluginSettings.getParentSelector().getParent(id, type);
    }

    private boolean hasParent(String type) {
        return pluginSettings.getParentSelector().typeHasParent(type);
    }

    private boolean shouldIgnoreDeletes(String index) {
        return pluginSettings.getIgnoreDeletes() != null && pluginSettings.getIgnoreDeletes().contains(index);
    }

    private boolean shouldIgnoreFailures() {
        return pluginSettings.getIgnoreFailures();
    }

    private String routingField(String type) {
        return pluginSettings.getDocumentTypeRoutingFields() == null ? null
                : pluginSettings.getDocumentTypeRoutingFields().get(type);
    }

    private String type(String index, String documentId) {
        return pluginSettings.getTypeSelector().getType(index, documentId);
    }

    private void setRouting(IndexRequestBuilder indexBuilder, Map<String, Object> toBeIndexed, String id, String routingField) {
        if (routingField != null) {
            Object routing = JSONMapPath(toBeIndexed, routingField);
            if (routing instanceof String) {
                indexBuilder.setRouting((String) routing);
            } else {
                logger.warn("Unable to determine routing value from routing field {} for doc id {}", routingField, id);
            }
        }
    }

    private void setParent(IndexRequestBuilder indexBuilder, Map<String, Object> toBeIndexed, String id, String type) {
        Object parent = pluginSettings.getParentSelector().getParent(toBeIndexed, id, type);
        if (parent != null) {
            if (parent instanceof String) {
                logger.debug("Setting parent of document {} to {}", id, parent);
                indexBuilder.setParent((String) parent);
            } else {
                logger.warn("Unable to determine parent value from parent field {} for doc id {}", parent, id);
            }
        }
    }

    public boolean failureMessageAppearsFatal(String failureMessage) {
        return !failureMessage.contains("EsRejectedExecutionException");
    }

    @Override
    public Map<String, Object> getDocument(String database, String docId) {
        String index = getElasticSearchIndexNameFromDatabase(database);
        String type = pluginSettings.getTypeSelector().getType(index, docId);
        return getDocumentElasticSearch(getElasticSearchIndexNameFromDatabase(database), docId, type);
    }

    @Override
    public Map<String, Object> getLocalDocument(String database, String docId) {
        return getDocumentElasticSearch(getElasticSearchIndexNameFromDatabase(database), docId, pluginSettings.getCheckpointDocumentType());
    }

    protected Map<String, Object> getDocumentElasticSearch(String index, String docId, String docType) {
        GetResponse response = client.prepareGet(index, docType, docId).execute().actionGet();
        if (response != null && response.isExists()) {
            Map<String, Object> esDocument = response.getSourceAsMap();
            return (Map<String, Object>) esDocument.get("doc");
        }
        return null;
    }

    @Override
    public String storeDocument(String database, String docId, Map<String, Object> document) {
        String index = getElasticSearchIndexNameFromDatabase(database);
        String type = pluginSettings.getTypeSelector().getType(index, docId);
        return storeDocumentElasticSearch(getElasticSearchIndexNameFromDatabase(database), docId, document, type);
    }

    @Override
    public String storeLocalDocument(String database, String docId,
                                     Map<String, Object> document) {
        return storeDocumentElasticSearch(getElasticSearchIndexNameFromDatabase(database), docId, document, pluginSettings.getCheckpointDocumentType());
    }

    protected String storeDocumentElasticSearch(String index, String docId, Map<String, Object> document, String docType) {
        // normally we just use the revision number present in the document
        String documentRevision = (String) document.get("_rev");
        if (documentRevision == null) {
            // if there isn't one we need to generate a revision number
            documentRevision = generateRevisionNumber();
            document.put("_rev", documentRevision);
        }
        IndexRequestBuilder indexBuilder = client.prepareIndex(index, docType, docId);
        indexBuilder.setSource(document);
        IndexResponse response = indexBuilder.execute().actionGet();
        if (response != null) {
            return documentRevision;
        }
        return null;
    }

    protected String generateRevisionNumber() {
        return "1-" + UUID.randomUUID().toString();
    }

    @Override
    public InputStream getAttachment(String database, String docId,
                                     String attachmentName) {
        throw new UnsupportedOperationException("Attachments are not supported");
    }

    @Override
    public String storeAttachment(String database, String docId,
                                  String attachmentName, String contentType, InputStream input) {
        throw new UnsupportedOperationException("Attachments are not supported");
    }

    @Override
    public InputStream getLocalAttachment(String database, String docId,
                                          String attachmentName) {
        throw new UnsupportedOperationException("Attachments are not supported");
    }

    @Override
    public String storeLocalAttachment(String database, String docId,
                                       String attachmentName, String contentType, InputStream input) {
        throw new UnsupportedOperationException("Attachments are not supported");
    }

    protected String getElasticSearchIndexNameFromDatabase(String database) {
        String[] pieces = database.split("/", 2);
        if (pieces.length < 2) {
            return database;
        } else {
            return pieces[0];
        }
    }

    protected String getBucketUUIDFromDatabase(String database) {
        String[] pieces = database.split(";", 2);
        if (pieces.length < 2) {
            return null;
        } else {
            return pieces[1];
        }
    }

    protected String getDatabaseNameWithoutUUID(String database) {
        int semicolonIndex = database.indexOf(';');
        if (semicolonIndex >= 0) {
            return database.substring(0, semicolonIndex);
        }
        return database;
    }

    public Map<String, Object> getStats() {
        Map<String, Object> stats = mapper.convertValue(metricRegistry, Map.class);
        stats.remove("version");
        return stats;
    }

    protected String getUUIDFromCheckpointDocSource(Map<String, Object> source) {
        Map<String, Object> docMap = (Map<String, Object>) source.get("doc");
        return (String) docMap.get("uuid");
    }

    protected String lookupUUID(String bucket, String id) {
        GetRequestBuilder builder = client.prepareGet();
        builder.setIndex(bucket);
        builder.setId(id);
        builder.setType(pluginSettings.getCheckpointDocumentType());
        builder.setFetchSource(true);

        String bucketUUID = null;
        GetResponse response;
        ActionFuture<GetResponse> laf = builder.execute();
        if (laf != null) {
            response = laf.actionGet();
            if (response.isExists()) {
                Map<String, Object> responseMap = response.getSourceAsMap();
                bucketUUID = this.getUUIDFromCheckpointDocSource(responseMap);
            }
        }

        return bucketUUID;
    }

    protected void storeUUID(String bucket, String id, String uuid) {
        Map<String, Object> doc = new HashMap<>();
        doc.put("uuid", uuid);
        Map<String, Object> toBeIndexed = new HashMap<>();
        toBeIndexed.put("doc", doc);

        IndexRequestBuilder builder = client.prepareIndex();
        builder.setIndex(bucket);
        builder.setId(id);
        builder.setType(pluginSettings.getCheckpointDocumentType());
        builder.setSource(toBeIndexed);
        builder.setOpType(IndexRequest.OpType.CREATE);

        IndexResponse response;
        ActionFuture<IndexResponse> laf = builder.execute();
        if (laf != null) {
            response = laf.actionGet();
            if (!isCreated(response)) {
                logger.error("did not succeed creating uuid");
            }
        }
    }

    public String getVBucketUUID(String pool, String bucket, int vbucket) {
        IndicesExistsRequestBuilder existsBuilder = client.admin().indices().prepareExists(bucket);
        IndicesExistsResponse response = existsBuilder.execute().actionGet();
        if (response.isExists()) {
            int tries = 0;
            String key = "vbucket" + vbucket + "UUID";
            String bucketUUID = this.lookupUUID(bucket, key);
            while (bucketUUID == null && tries < 100) {
                logger.debug("vbucket {} UUID doesn't exist yet,  creating", vbucket);
                String newUUID = UUID.randomUUID().toString().replace("-", "");
                storeUUID(bucket, key, newUUID);
                bucketUUID = this.lookupUUID(bucket, key);
                tries++;
            }

            if (bucketUUID == null) {
                throw new RuntimeException("failed to find/create bucket uuid after 100 tries");
            }

            return bucketUUID;
        }
        return null;
    }

    @Override
    public String getBucketUUID(String pool, String bucket) {
        // first look for bucket UUID in cache
        String bucketUUID = this.bucketUUIDCache.getIfPresent(bucket);
        if (bucketUUID != null) {
            logger.debug("found bucket UUID in cache");
            return bucketUUID;
        }

        logger.debug("bucket UUID not in cache, looking up");
        IndicesExistsRequestBuilder existsBuilder = client.admin().indices().prepareExists(bucket);
        IndicesExistsResponse response = existsBuilder.execute().actionGet();
        if (response.isExists()) {
            int tries = 0;
            bucketUUID = this.lookupUUID(bucket, "bucketUUID");
            while (bucketUUID == null && tries < 100) {
                logger.debug("bucket UUID doesn't exist yet, creaating, attempt: {}", tries + 1);
                String newUUID = UUID.randomUUID().toString().replace("-", "");
                storeUUID(bucket, "bucketUUID", newUUID);
                bucketUUID = this.lookupUUID(bucket, "bucketUUID");
                tries++;
            }

            if (bucketUUID != null) {
                // store it in the cache
                bucketUUIDCache.put(bucket, bucketUUID);
                return bucketUUID;
            }
        }
        throw new RuntimeException("failed to find/create bucket uuid");
    }

    public static Object JSONMapPath(Map<String, Object> json, String path) {
        int dotIndex = path.indexOf('.');
        if (dotIndex >= 0) {
            String pathThisLevel = path.substring(0, dotIndex);
            Object current = json.get(pathThisLevel);
            String pathRest = path.substring(dotIndex + 1);
            if (pathRest.length() == 0) {
                return current;
            } else if (current instanceof Map && pathRest.length() > 0) {
                return JSONMapPath((Map<String, Object>) current, pathRest);
            }
        } else {
            // no dot
            return json.get(path);
        }
        return null;
    }
}
