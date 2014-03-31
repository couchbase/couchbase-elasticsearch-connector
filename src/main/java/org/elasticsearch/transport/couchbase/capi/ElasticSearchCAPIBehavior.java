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

import javax.servlet.UnavailableException;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;

import com.couchbase.capi.CAPIBehavior;

public class ElasticSearchCAPIBehavior implements CAPIBehavior {

    protected ObjectMapper mapper = new ObjectMapper();
    protected Client client;
    protected ESLogger logger;

    protected String defaultDocumentType;
    protected String checkpointDocumentType;
    protected String dynamicTypePath;
    protected boolean resolveConflicts;

    protected CounterMetric activeRevsDiffRequests;
    protected MeanMetric meanRevsDiffRequests;
    protected CounterMetric activeBulkDocsRequests;
    protected MeanMetric meanBulkDocsRequests;
    protected CounterMetric totalTooManyConcurrentRequestsErrors;

    protected long maxConcurrentRequests;

    public ElasticSearchCAPIBehavior(Client client, ESLogger logger, String defaultDocumentType, String checkpointDocumentType, String dynamicTypePath, boolean resolveConflicts, long maxConcurrentRequests) {
        this.client = client;
        this.logger = logger;
        this.defaultDocumentType = defaultDocumentType;
        this.checkpointDocumentType = checkpointDocumentType;
        this.dynamicTypePath = dynamicTypePath;
        this.resolveConflicts = resolveConflicts;

        this.activeRevsDiffRequests = new CounterMetric();
        this.meanRevsDiffRequests = new MeanMetric();
        this.activeBulkDocsRequests = new CounterMetric();
        this.meanBulkDocsRequests = new MeanMetric();
        this.totalTooManyConcurrentRequestsErrors = new CounterMetric();

        this.maxConcurrentRequests = maxConcurrentRequests;
    }

    @Override
    public boolean databaseExists(String database) {
        String index = getElasticSearchIndexNameFromDatabase(database);
        IndicesExistsRequestBuilder existsBuilder = client.admin().indices().prepareExists(index);
        IndicesExistsResponse response = existsBuilder.execute().actionGet();
        if(response.isExists()) {
            return true;
        }
        return false;
    }

    @Override
    public Map<String, Object> getDatabaseDetails(String database) {
        if(databaseExists(database)) {
            Map<String, Object> responseMap = new HashMap<String, Object>();
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
        if(activeBulkDocsRequests.count() + activeRevsDiffRequests.count() >= maxConcurrentRequests) {
            totalTooManyConcurrentRequestsErrors.inc();
            throw new UnavailableException("Too many concurrent requests");
        }

        long start = System.currentTimeMillis();
        activeRevsDiffRequests.inc();
        logger.trace("_revs_diff request for {} : {}", database, revsMap);

        // start with all entries in the response map
        Map<String, Object> responseMap = new HashMap<String, Object>();
        for (Entry<String, Object> entry : revsMap.entrySet()) {
            String id = entry.getKey();
            String revs = (String)entry.getValue();
            Map<String, String> rev = new HashMap<String, String>();
            rev.put("missing", revs);
            responseMap.put(id, rev);
        }
        logger.trace("_revs_diff response for {} is: {}", database, responseMap);

        // if resolve conflicts mode is enabled
        // perform a multi-get query to find information
        // about revisions we already have
        if (resolveConflicts) {
            String index = getElasticSearchIndexNameFromDatabase(database);
            // the following debug code is verbose in the hopes of better understanding CBES-13
            MultiGetResponse response = null;
            if(client != null) {
                MultiGetRequestBuilder builder = client.prepareMultiGet();
                if(builder != null) {
                    if(index == null) {
                        logger.debug("index is null");
                    }
                    if(defaultDocumentType == null) {
                        logger.debug("default document type is null");
                    }
                    builder = builder.add(index, defaultDocumentType, responseMap.keySet());
                    if(builder != null) {
                        ListenableActionFuture<MultiGetResponse> laf = builder.execute();
                        if(laf != null) {
                            response = laf.actionGet();
                        } else {
                            logger.debug("laf was null");
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
                Iterator<MultiGetItemResponse> iterator = response.iterator();
                while(iterator.hasNext()) {
                    MultiGetItemResponse item = iterator.next();
                    if(item.getResponse().isExists()) {
                        String itemId = item.getId();
                        Map<String, Object> source = item.getResponse().getSourceAsMap();
                        if(source != null) {
                            Map<String, Object> meta = (Map<String, Object>)source.get("meta");
                            if(meta != null) {
                                String rev = (String)meta.get("rev");
                                //retrieve the revision passed in from Couchbase
                                Map<String, String> sourceRevMap = (Map<String, String>)responseMap.get(itemId);
                                String sourceRev = sourceRevMap.get("missing");
                                if(rev.equals(sourceRev)) {
                                    // if our revision is the same as the source rev
                                    // remove it from the response map
                                    responseMap.remove(itemId);
                                    logger.trace("_revs_diff already have id: {} rev: {}", itemId, rev);
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
        if(activeBulkDocsRequests.count() + activeRevsDiffRequests.count() >= maxConcurrentRequests) {
            totalTooManyConcurrentRequestsErrors.inc();
            throw new UnavailableException("Too many concurrent requests");
        }

        long start = System.currentTimeMillis();
        activeBulkDocsRequests.inc();
        String index = getElasticSearchIndexNameFromDatabase(database);


        BulkRequestBuilder bulkBuilder = client.prepareBulk();

        // keep a map of the id - rev for building the response
        Map<String,String> revisions = new HashMap<String, String>();

        logger.debug("Bulk doc entry is {}", docs);
        for (Map<String, Object> doc : docs) {

            // these are the top-level elements that could be in the document sent by Couchbase
            Map<String, Object> meta = (Map<String, Object>)doc.get("meta");
            Map<String, Object> json = (Map<String, Object>)doc.get("json");
            String base64 = (String)doc.get("base64");

            if(meta == null) {
                // if there is no meta-data section, there is nothing we can do
                logger.warn("Document without meta in bulk_docs, ignoring....");
                continue;
            } else if("non-JSON mode".equals(meta.get("att_reason"))) {
                // optimization, this tells us the body isn't json
                json = new HashMap<String, Object>();
            } else if(json == null && base64 != null) {
                // no plain json, let's try parsing the base64 data
                try {
                    byte[] decodedData = Base64.decode(base64);
                    try {
                        // now try to parse the decoded data as json
                        json = (Map<String, Object>) mapper.readValue(decodedData, Map.class);
                    }
                    catch(IOException e) {
                        logger.error("Unable to parse decoded base64 data as JSON, indexing stub for id: {}", meta.get("id"));
                        logger.error("Body was: {} Parse error was: {}", new String(decodedData), e);
                        json = new HashMap<String, Object>();

                    }
                } catch (IOException e) {
                    logger.error("Unable to decoded base64, indexing stub for id: {}", meta.get("id"));
                    logger.error("Base64 was was: {} Parse error was: {}", base64, e);
                    json = new HashMap<String, Object>();
                }
            }

            // at this point we know we have the document meta-data
            // and the document contents to be indexed are in json

            Map<String, Object> toBeIndexed = new HashMap<String, Object>();
            toBeIndexed.put("meta", meta);
            toBeIndexed.put("doc", json);

            String id = (String)meta.get("id");
            String rev = (String)meta.get("rev");
            revisions.put(id, rev);

            long ttl = 0;
            Integer expiration = (Integer)meta.get("expiration");
            if(expiration != null) {
                ttl = (expiration.longValue() * 1000) - System.currentTimeMillis();
            }


            String type = defaultDocumentType;
            if(id.startsWith("_local/")) {
                type = checkpointDocumentType;
            }
            boolean deleted = meta.containsKey("deleted") ? (Boolean)meta.get("deleted") : false;

            if(deleted) {
                DeleteRequest deleteRequest = client.prepareDelete(index, type, id).request();
                bulkBuilder.add(deleteRequest);
            } else {
                IndexRequestBuilder indexBuilder = client.prepareIndex(index, type, id);
                indexBuilder.setSource(toBeIndexed);
                if(ttl > 0) {
                    indexBuilder.setTTL(ttl);
                }
                IndexRequest indexRequest = indexBuilder.request();
                bulkBuilder.add(indexRequest);
            }
        }

        List<Object> result = new ArrayList<Object>();


        BulkResponse response = bulkBuilder.execute().actionGet();
        if(response != null) {
            for (BulkItemResponse bulkItemResponse : response.getItems()) {
                Map<String, Object> itemResponse = new HashMap<String, Object>();
                String itemId = bulkItemResponse.getId();
                itemResponse.put("id", itemId);
                if(bulkItemResponse.isFailed()) {
                    itemResponse.put("error", "failed");
                    itemResponse.put("reason", bulkItemResponse.getFailureMessage());
                    logger.error("indexing error for id: {} reason: {}", itemId, bulkItemResponse.getFailureMessage());
                    throw new RuntimeException("indexing error " + bulkItemResponse.getFailureMessage());
                } else {
                    itemResponse.put("rev", revisions.get(itemId));
                }
                result.add(itemResponse);
            }
        }

        long end = System.currentTimeMillis();
        meanBulkDocsRequests.inc(end - start);
        activeBulkDocsRequests.dec();
        return result;
    }

    @Override
    public Map<String, Object> getDocument(String database, String docId) {
        return getDocumentElasticSearch(getElasticSearchIndexNameFromDatabase(database), docId, defaultDocumentType);
    }

    @Override
    public Map<String, Object> getLocalDocument(String database, String docId) {
        return getDocumentElasticSearch(getElasticSearchIndexNameFromDatabase(database), docId, checkpointDocumentType);
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
        return storeDocumentElasticSearch(getElasticSearchIndexNameFromDatabase(database), docId, document, defaultDocumentType);
    }

    @Override
    public String storeLocalDocument(String database, String docId,
            Map<String, Object> document) {
        return storeDocumentElasticSearch(getElasticSearchIndexNameFromDatabase(database), docId, document, checkpointDocumentType);
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
        String documentRevision = "1-" + UUID.randomUUID().toString();
        return documentRevision;
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
    public InputStream getLocalAttachment(String databsae, String docId,
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

    protected String getDatabaseNameWithoutUUID(String database) {
        int semicolonIndex = database.indexOf(';');
        if(semicolonIndex >= 0) {
            return database.substring(0, semicolonIndex);
        }
        return database;
    }

    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<String, Object>();

        Map<String, Object> bulkDocsStats = new HashMap<String, Object>();
        bulkDocsStats.put("activeCount", activeBulkDocsRequests.count());
        bulkDocsStats.put("totalCount", meanBulkDocsRequests.count());
        bulkDocsStats.put("totalTime", meanBulkDocsRequests.sum());
        bulkDocsStats.put("avgTime", meanBulkDocsRequests.mean());

        Map<String, Object> revsDiffStats = new HashMap<String, Object>();
        revsDiffStats.put("activeCount", activeRevsDiffRequests.count());
        revsDiffStats.put("totalCount", meanRevsDiffRequests.count());
        revsDiffStats.put("totalTime", meanRevsDiffRequests.sum());
        revsDiffStats.put("avgTime", meanRevsDiffRequests.mean());

        stats.put("_bulk_docs", bulkDocsStats);
        stats.put("_revs_diff", revsDiffStats);
        stats.put("tooManyConcurrentRequestsErrors", totalTooManyConcurrentRequestsErrors.count());

        return stats;
    }
}
