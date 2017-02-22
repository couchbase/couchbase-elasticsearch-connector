/**
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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.Base64;

import javax.servlet.UnavailableException;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.ListenableActionFuture;
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
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import com.google.common.cache.Cache;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;

import com.couchbase.capi.CAPIBehavior;
import org.elasticsearch.rest.RestStatus;

public class ElasticSearchCAPIBehavior implements CAPIBehavior {

    protected ObjectMapper mapper = new ObjectMapper();
    protected Client client;
    protected Logger logger;

    protected CounterMetric activeRevsDiffRequests;
    protected MeanMetric meanRevsDiffRequests;
    protected CounterMetric activeBulkDocsRequests;
    protected MeanMetric meanBulkDocsRequests;
    protected CounterMetric totalTooManyConcurrentRequestsErrors;

    protected Cache<String, String> bucketUUIDCache;

    protected PluginSettings pluginSettings;

    public ElasticSearchCAPIBehavior(Client client, Logger logger, Cache<String, String> bucketUUIDCache, PluginSettings pluginSettings) {
        this.client = client;
        this.logger = logger;
        this.pluginSettings = pluginSettings;

        this.activeRevsDiffRequests = new CounterMetric();
        this.meanRevsDiffRequests = new MeanMetric();
        this.activeBulkDocsRequests = new CounterMetric();
        this.meanBulkDocsRequests = new MeanMetric();
        this.totalTooManyConcurrentRequestsErrors = new CounterMetric();

        this.bucketUUIDCache = bucketUUIDCache;
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
        if(response.isExists()) {
            String uuid = getBucketUUIDFromDatabase(database);
            if(uuid != null) {
                logger.debug("included uuid, validating");
                String actualUUID = getBucketUUID("default", index);
                if(!uuid.equals(actualUUID)) {
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
        if(doesNotExistReason == null) {
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

    @Override
    public Map<String, Object> revsDiff(String database,
            Map<String, Object> revsMap)  throws UnavailableException {
        // check to see if too many requests are already active
        if(activeBulkDocsRequests.count() + activeRevsDiffRequests.count() >= pluginSettings.getMaxConcurrentRequests()) {
            totalTooManyConcurrentRequestsErrors.inc();
            logger.error("Too many concurrent requests. _bulk_docs requests: {}, _revs_diff requests: {}, Max configured: {}",
                    activeBulkDocsRequests.count(),
                    activeRevsDiffRequests.count(),
                    pluginSettings.getMaxConcurrentRequests());
            throw new UnavailableException("Too many concurrent requests");
        }

        long start = System.currentTimeMillis();
        activeRevsDiffRequests.inc();
        logger.trace("_revs_diff request for {} : {}", database, revsMap);

        // start with all entries in the response map
        Map<String, Object> responseMap = new HashMap<>();
        for (Entry<String, Object> entry : revsMap.entrySet()) {
            String id = entry.getKey();
            String revs = (String)entry.getValue();
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
            if(client != null) {
                MultiGetRequestBuilder builder = client.prepareMultiGet();
                if(builder != null) {
                    if(index == null) {
                        logger.debug("index is null");
                    }
                    int added = 0;
                    for (String id : responseMap.keySet()) {
                        String type = pluginSettings.getTypeSelector().getType(index, id);
                        if(pluginSettings.getDocumentTypeRoutingFields() != null && pluginSettings.getDocumentTypeRoutingFields().containsKey(type)) {
                            // if this type requires special routing, we can't find it without the doc body
                            // so we skip this id in the lookup to avoid errors
                            continue;
                        }
                        builder = builder.add(index, type, id);
                        added++;
                    }
                    if(builder != null) {
                        if(added > 0) {
                            ListenableActionFuture<MultiGetResponse> laf = builder.execute();
                            if(laf != null) {
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
            if(response != null) {
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

        long end = System.currentTimeMillis();
        meanRevsDiffRequests.inc(end - start);
        activeRevsDiffRequests.dec();
        return responseMap;
    }

    @Override
    public List<Object> bulkDocs(String database, List<Map<String, Object>> docs) throws UnavailableException {
        // check to see if too many requests are already active
        if(activeBulkDocsRequests.count() + activeRevsDiffRequests.count() >= pluginSettings.getMaxConcurrentRequests()) {
            totalTooManyConcurrentRequestsErrors.inc();
            logger.error("Too many concurrent requests. _bulk_docs requests: {}, _revs_diff requests: {}, Max configured: {}",
                    activeBulkDocsRequests.count(),
                    activeRevsDiffRequests.count(),
                    pluginSettings.getMaxConcurrentRequests());
            throw new UnavailableException("Too many concurrent requests");
        }

        long start = System.currentTimeMillis();
        activeBulkDocsRequests.inc();
        String index = getElasticSearchIndexNameFromDatabase(database);

		//if set to true - all delete operations will be ignored
		//ignoreDeletes contains a list of indexes to be ignored when delete events occur
		//index list can be set in the elasticsearch.yml file using
		//the key: couchbase.ignore.delete  the value is colon separated:  index1:index2:index3 
		boolean ignoreDelete = pluginSettings.getIgnoreDeletes() != null && pluginSettings.getIgnoreDeletes().contains(index);
        logger.trace("ignoreDelete = {}", ignoreDelete);
        
        // keep a map of the id - rev for building the response
        Map<String,String> revisions = new HashMap<>();

        // put requests into this map, not directly into the bulk request
        Map<String,IndexRequest> bulkIndexRequests = new HashMap<>();
        Map<String,DeleteRequest> bulkDeleteRequests = new HashMap<>();
        
        //used for "mock" results in case of ignore deletes or filtered out keys
        List<Object> mockResults = new ArrayList<>();

        logger.trace("Bulk doc entry is {}", docs);
        for (Map<String, Object> doc : docs) {

            // these are the top-level elements that could be in the document sent by Couchbase
            Map<String, Object> meta = (Map<String, Object>)doc.get("meta");
            Map<String, Object> json = (Map<String, Object>)doc.get("json");
            String base64 = (String)doc.get("base64");

            if(meta == null) {
                // if there is no meta-data section, there is nothing we can do
                logger.warn("Document without meta in bulk_docs, ignoring....");
                continue;
            }

            String id = (String)meta.get("id");
            String rev = (String)meta.get("rev");

            if(id == null) {
                // if there is no id in the metadata, something is seriously wrong
                logger.warn("Document metadata does not have an id, ignoring...");
                continue;
            }

            // Filter documents by ID.
            // Delete operations are always allowed through to ES, to make sure newly configured
            // filters don't cause documents to stay in ES forever.
            if(!pluginSettings.getKeyFilter().shouldAllow(index, id) && !meta.containsKey("deleted")) {
                // Document ID matches one of the filters, not passing it to on to ES.
                // Store a mock response, which will be added to the responses sent back
                // to Couchbase, to satisfy the XDCR mechanism
                Map<String, Object> mockResponse = new HashMap<>();
                mockResponse.put("id", id);
                mockResponse.put("rev", rev);
                mockResults.add(mockResponse);

                logger.trace("Document doesn't pass configured key filters, not storing: {}", id);
                continue;
            }

            if(meta.containsKey("deleted")) {
                // if this is only a delete anyway, don't bother looking at the body
                json = new HashMap<>();
            } else if("non-JSON mode".equals(meta.get("att_reason")) || "invalid_json".equals(meta.get("att_reason"))) {
                // optimization, this tells us the body isn't json
                json = new HashMap<>();
            } else if(json == null && base64 != null) {
                // no plain json, let's try parsing the base64 data
                try {
                    byte[] decodedData = Base64.getDecoder().decode(base64);
                    try {
                        // now try to parse the decoded data as json
                        json = (Map<String, Object>) mapper.readValue(decodedData, Map.class);
                    }
                    catch(IOException e) {
                        json = new HashMap<>();
                        if(pluginSettings.getWrapCounters()) {
                            logger.trace("Trying to parse decoded base64 data as a long and wrap it as a counter document, id: {}", meta.get("id"));
                            try {
                                long value = Long.parseLong(new String(decodedData));
                                logger.trace("Parsed data as long: {}", value);
                                json.put("value", value);
                            }
                            catch(Exception e2) {
                                logger.error("Unable to parse decoded base64 data as either JSON or long, indexing stub for id: {}", meta.get("id"));
                                logger.error("Body was: {} Parse error was: {} Long parse error was: {}", new String(decodedData), e, e2);
                            }
                        }
                        else {
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

            Map<String, Object> toBeIndexed = new HashMap<>();
            toBeIndexed.put("meta", meta);
            toBeIndexed.put("doc", json);

            revisions.put(id, rev);

            long ttl = 0;
            Integer expiration = (Integer)meta.get("expiration");
            if(expiration != null && expiration > 0) {
                ttl = (expiration.longValue() * 1000) - System.currentTimeMillis();
                if(logger.isDebugEnabled())
                    logger.debug(String.format("Document %s has expiration set, which is not supported in ES 5.0+", id));
            }

            String routingField = null;
            String type = pluginSettings.getTypeSelector().getType(index, id);
            logger.trace("Selecting type {} for document {} in index {}", type, id, index);

            if(pluginSettings.getDocumentTypeRoutingFields() != null && pluginSettings.getDocumentTypeRoutingFields().containsKey(type)) {
                routingField = pluginSettings.getDocumentTypeRoutingFields().get(type);
                logger.trace("Using {} as the routing field for document type {}", routingField, type);
            }
            boolean deleted = meta.containsKey("deleted") ? (Boolean)meta.get("deleted") : false;
            
            if(deleted) {
            	if (!ignoreDelete) {
                	DeleteRequest deleteRequest = client.prepareDelete(index, type, id).request();
                	bulkDeleteRequests.put(id, deleteRequest);
            	}else{
                	// For ignored deletes, we want to bypass from adding the delete request
            		// as a hack - we add a "mock" response for each delete request as if ES returned
            		// delete confirmation
	                Map<String, Object> mockResponse = new HashMap<>();
	                mockResponse.put("id", id);
	                mockResponse.put("rev", rev);
	                mockResults.add(mockResponse);
            	}
            } else {
                IndexRequestBuilder indexBuilder = client.prepareIndex(index, type, id);
                indexBuilder.setSource(toBeIndexed);
                if(!ignoreDelete && ttl > 0) {
                    indexBuilder.setTTL(ttl);
                }
                Object parent = pluginSettings.getParentSelector().getParent(toBeIndexed, id, type);
                if (parent != null) {
                    if (parent instanceof String) {
                        logger.debug("Setting parent of document {} to {}", id, parent);
                        indexBuilder.setParent((String) parent);
                    } else {
                        logger.warn("Unable to determine parent value from parent field {} for doc id {}", parent, id);
                    }
                }
                if(routingField != null) {
                    Object routing = JSONMapPath(toBeIndexed, routingField);
                    if (routing != null && routing instanceof String) {
                        indexBuilder.setRouting((String)routing);
                    } else {
                        logger.warn("Unable to determine routing value from routing field {} for doc id {}", routingField, id);
                    }
                }
                IndexRequest indexRequest = indexBuilder.request();
                bulkIndexRequests.put(id,  indexRequest);
            }
        }

        int attempt = 0;
        BulkResponse response = null;
        List<Object> result;
        long retriesLeft = pluginSettings.getBulkIndexRetries();
        StringBuilder errors = new StringBuilder();
        BulkRequestBuilder bulkBuilder;

        do {
            // build the bulk request for this iteration
            bulkBuilder = client.prepareBulk();
            for (Entry<String,IndexRequest> entry : bulkIndexRequests.entrySet()) {
                bulkBuilder.add(entry.getValue());
            }
            for (Entry<String,DeleteRequest> entry : bulkDeleteRequests.entrySet()) {
                bulkBuilder.add(entry.getValue());
            }

            attempt++;
            result = new ArrayList<>();

            if(bulkBuilder.numberOfActions() > 0) {
                if(response != null) { // at least second time through
                    try {
                        Thread.sleep(pluginSettings.getBulkIndexRetryWaitMs());
                    } catch (InterruptedException e) {
                        errors.append(e.toString());
                        break;
                    }
                }
                response = bulkBuilder.execute().actionGet();
            }
            else
                break;
            
            if(response != null) {
                for (BulkItemResponse bulkItemResponse : response.getItems()) {
                    String itemId = bulkItemResponse.getId();
                    String itemRev = revisions.get(itemId);

                    if(!bulkItemResponse.isFailed()) {
                        Map<String, Object> itemResponse = new HashMap<>();
                        itemResponse.put("id", itemId);
                        itemResponse.put("rev", itemRev);
                        result.add(itemResponse);

                        // remove the item from the bulk requests list so we don't try to index it again
                        bulkIndexRequests.remove(itemId);
                        bulkDeleteRequests.remove(itemId);
                    } else {
                        Failure failure = bulkItemResponse.getFailure();

                        // If the error is fatal, don't retry the request.
                        if(failureMessageAppearsFatal(failure.getMessage())) {
                            logger.error("Error indexing document, will NOT retry. Document id: " + itemId + " exception: " + failure.getMessage());

                            bulkIndexRequests.remove(itemId);
                            bulkDeleteRequests.remove(itemId);

                            // If ignore failures mode is on, store a mock result object for the failed
                            // operation, which will be returned to Couchbase.
                            if(pluginSettings.getIgnoreFailures()) {
                                Map<String, Object> mockResult = new HashMap<>();
                                mockResult.put("id", itemId);
                                mockResult.put("rev", itemRev);
                                mockResults.add(mockResult);
                            }
                            else {
                                errors.append(failure.getMessage());
                                errors.append(System.lineSeparator());
                            }
                        }
                        else {
                            logger.warn("Error indexing document, will retry. Document id: " + itemId + " exception: " + failure.getMessage());
                        }
                    }
                }
            }
            retriesLeft--;

        } while(response != null && response.hasFailures() && retriesLeft > 0);

        long end = System.currentTimeMillis();
        meanBulkDocsRequests.inc(end - start);
        activeBulkDocsRequests.dec();

        if(response == null && bulkBuilder != null && bulkBuilder.numberOfActions() > 0)
            errors.append("Indexing error: bulk index response was null" + System.lineSeparator());
        if(retriesLeft == 0)
            errors.append("Indexing error: bulk index failed after all retries" + System.lineSeparator());

        if(errors.length() > 0)
            if(pluginSettings.getIgnoreFailures())
                logger.error(errors.toString());
            else
                throw new RuntimeException(errors.toString());
        else
            if(attempt == 1)
                logger.debug("Bulk index succeeded after {} tries", attempt);
            else
                logger.warn("Bulk index succeeded after {} tries", attempt);

        // Before we return, in case of ignore delete or filtered keys
        // we want to add the "mock" confirmations for the ignored operations
        // in order to satisfy the XDCR mechanism
        if(mockResults != null && mockResults.size() > 0)
        	result.addAll(mockResults);

        return result;
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
        if(response != null && response.isExists()) {
            Map<String,Object> esDocument = response.getSourceAsMap();
            return (Map<String, Object>)esDocument.get("doc");
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
        String documentRevision = (String)document.get("_rev");
        if(documentRevision == null) {
            // if there isn't one we need to generate a revision number
            documentRevision = generateRevisionNumber();
            document.put("_rev", documentRevision);
        }
        IndexRequestBuilder indexBuilder = client.prepareIndex(index, docType, docId);
        indexBuilder.setSource(document);
        IndexResponse response = indexBuilder.execute().actionGet();
        if(response != null) {
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
        if(pieces.length < 2) {
            return database;
        } else {
            return pieces[0];
        }
    }

    protected String getBucketUUIDFromDatabase(String database) {
        String[] pieces = database.split(";", 2);
        if(pieces.length < 2) {
            return null;
        } else {
            return pieces[1];
        }
    }

    protected String getDatabaseNameWithoutUUID(String database) {
        int semicolonIndex = database.indexOf(';');
        if(semicolonIndex >= 0) {
            return database.substring(0, semicolonIndex);
        }
        return database;
    }

    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();

        Map<String, Object> bulkDocsStats = new HashMap<>();
        bulkDocsStats.put("activeCount", activeBulkDocsRequests.count());
        bulkDocsStats.put("totalCount", meanBulkDocsRequests.count());
        bulkDocsStats.put("totalTime", meanBulkDocsRequests.sum());
        bulkDocsStats.put("avgTime", meanBulkDocsRequests.mean());

        Map<String, Object> revsDiffStats = new HashMap<>();
        revsDiffStats.put("activeCount", activeRevsDiffRequests.count());
        revsDiffStats.put("totalCount", meanRevsDiffRequests.count());
        revsDiffStats.put("totalTime", meanRevsDiffRequests.sum());
        revsDiffStats.put("avgTime", meanRevsDiffRequests.mean());

        stats.put("_bulk_docs", bulkDocsStats);
        stats.put("_revs_diff", revsDiffStats);
        stats.put("tooManyConcurrentRequestsErrors", totalTooManyConcurrentRequestsErrors.count());

        return stats;
    }

    protected String getUUIDFromCheckpointDocSource(Map<String, Object> source) {
        Map<String,Object> docMap = (Map<String,Object>)source.get("doc");
        return (String)docMap.get("uuid");
    }

    protected String lookupUUID(String bucket, String id) {
        GetRequestBuilder builder = client.prepareGet();
        builder.setIndex(bucket);
        builder.setId(id);
        builder.setType(pluginSettings.getCheckpointDocumentType());
        builder.setFetchSource(true);

        String bucketUUID = null;
        GetResponse response;
        ListenableActionFuture<GetResponse> laf = builder.execute();
        if(laf != null) {
            response = laf.actionGet();
            if(response.isExists()) {
            Map<String,Object> responseMap = response.getSourceAsMap();
            bucketUUID = this.getUUIDFromCheckpointDocSource(responseMap);
            }
        }

        return bucketUUID;
    }

    protected void storeUUID(String bucket, String id, String uuid) {
        Map<String,Object> doc = new HashMap<>();
        doc.put("uuid", uuid);
        Map<String, Object> toBeIndexed = new HashMap<>();
        toBeIndexed.put("doc", doc);

        IndexRequestBuilder builder = client.prepareIndex();
        builder.setIndex(bucket);
        builder.setId(id);
        builder.setType(pluginSettings.getCheckpointDocumentType());
        builder.setSource(toBeIndexed);
        builder.setOpType(OpType.CREATE);

        IndexResponse response;
        ListenableActionFuture<IndexResponse> laf = builder.execute();
        if(laf != null) {
            response = laf.actionGet();
            if(response.status() != RestStatus.CREATED) {
                logger.error("did not succeed creating uuid");
            }
        }
    }

    public String getVBucketUUID(String pool, String bucket, int vbucket) {
        IndicesExistsRequestBuilder existsBuilder = client.admin().indices().prepareExists(bucket);
        IndicesExistsResponse response = existsBuilder.execute().actionGet();
        if(response.isExists()) {
            int tries = 0;
            String key = String.format("vbucket%dUUID",vbucket);
            String bucketUUID = this.lookupUUID(bucket, key);
            while(bucketUUID == null && tries < 100) {
                logger.debug("vbucket {} UUID doesn't exist yet,  creating", vbucket);
                String newUUID = UUID.randomUUID().toString().replace("-", "");
                storeUUID(bucket, key, newUUID);
                bucketUUID = this.lookupUUID(bucket, key);
                tries++;
            }

            if(bucketUUID == null) {
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
        if(response.isExists()) {
            int tries = 0;
            bucketUUID = this.lookupUUID(bucket, "bucketUUID");
            while(bucketUUID == null && tries < 100) {
                logger.debug("bucket UUID doesn't exist yet, creaating, attempt: {}", tries+1);
                String newUUID = UUID.randomUUID().toString().replace("-", "");
                storeUUID(bucket, "bucketUUID", newUUID);
                bucketUUID = this.lookupUUID(bucket, "bucketUUID");
                tries++;
            }

            if(bucketUUID != null) {
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
            String pathThisLevel = path.substring(0,dotIndex);
            Object current = json.get(pathThisLevel);
            String pathRest = path.substring(dotIndex+1);
            if (pathRest.length() == 0) {
                return current;
            }
            else if(current instanceof Map && pathRest.length() > 0) {
                return JSONMapPath((Map<String, Object>)current, pathRest);
            }
        } else {
            // no dot
            return json.get(path);
        }
        return null;
    }
}
