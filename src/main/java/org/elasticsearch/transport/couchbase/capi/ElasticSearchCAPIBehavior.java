package org.elasticsearch.transport.couchbase.capi;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.admin.indices.exists.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.logging.ESLogger;

import com.couchbase.capi.CAPIBehavior;

public class ElasticSearchCAPIBehavior implements CAPIBehavior {

    public static final String DOCUMENT_TYPE_DOCUMENT = "couchbaseDocument";
    public static final String DOCUMENT_TYPE_CHECKPOINT = "couchbaseCheckpoint";
    public static final String COUCHBASE_MASTER_DB_SUFFIX = "master";

    protected ObjectMapper mapper = new ObjectMapper();
    protected Client client;
    protected ESLogger logger;

    public ElasticSearchCAPIBehavior(Client client, ESLogger logger) {
        this.client = client;
        this.logger = logger;
    }

    @Override
    public boolean databaseExists(String database) {
        String index = getElasticSearchIndexNameFromDatabase(database);
        IndicesExistsRequestBuilder existsBuilder = client.admin().indices().prepareExists(index);
        IndicesExistsResponse response = existsBuilder.execute().actionGet();
        if(response.exists()) {
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
            Map<String, Object> revsMap) {

        Map<String, Object> responseMap = new HashMap<String, Object>();
        for (Entry<String, Object> entry : revsMap.entrySet()) {
            String id = entry.getKey();
            Object revs = entry.getValue();
            Map<String, Object> rev = new HashMap<String, Object>();
            rev.put("missing", revs);
            responseMap.put(id, rev);
        }
        return responseMap;
    }

    @Override
    public List<Object> bulkDocs(String database, List<Map<String, Object>> docs) {
        String index = getElasticSearchIndexNameFromDatabase(database);


        BulkRequestBuilder bulkBuilder = client.prepareBulk();

        // keep a map of the id - rev for building the response
        Map<String,String> revisions = new HashMap<String, String>();

        for (Map<String, Object> doc : docs) {

            logger.debug("Bulk doc entry is {}", docs);

            // these are the top-level elements that could be in the document sent by Couchbase
            Map<String, Object> meta = (Map<String, Object>)doc.get("meta");
            Map<String, Object> json = (Map<String, Object>)doc.get("json");
            String base64 = (String)doc.get("base64");

            if(meta == null) {
                // if there is no meta-data section, there is nothing we can do
                logger.warn("Document without meta in bulk_docs, ignoring....");
                continue;
            } else if("non-JSON mode".equals(meta.get("attr_reason"))) {
                // optimization, this tells us the body isn't json
                json = new HashMap<String, Object>();
            } else if(json == null && base64 != null) {
                // no plain json, let's try parsing the base64 data
                try {
                    byte[] decodedData = Base64.decode(base64);
                    // now try to parse the decoded data as json
                    json = (Map<String, Object>) mapper.readValue(decodedData, Map.class);
                } catch (IOException e) {
                    logger.warn("Unable to parse decoded base64 data as JSON, indexing stub...");
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

            String type = DOCUMENT_TYPE_DOCUMENT;
            if(id.startsWith("_local/")) {
                type = DOCUMENT_TYPE_CHECKPOINT;
            }
            boolean deleted = meta.containsKey("deleted") ? (Boolean)meta.get("deleted") : false;

            if(deleted) {
                DeleteRequest deleteRequest = client.prepareDelete(index, type, id).request();
                bulkBuilder.add(deleteRequest);
            } else {
                IndexRequestBuilder indexBuilder = client.prepareIndex(index, type, id);
                indexBuilder.setSource(toBeIndexed);
                IndexRequest indexRequest = indexBuilder.request();
                bulkBuilder.add(indexRequest);
            }
        }

        List<Object> result = new ArrayList<Object>();

        BulkResponse response = bulkBuilder.execute().actionGet();
        if(response != null) {
            for (BulkItemResponse bulkItemResponse : response.items()) {
                Map<String, Object> itemResponse = new HashMap<String, Object>();
                String itemId = bulkItemResponse.getId();
                itemResponse.put("id", itemId);
                if(bulkItemResponse.failed()) {
                    itemResponse.put("error", "failed");
                    itemResponse.put("reason", bulkItemResponse.failureMessage());
                } else {
                    itemResponse.put("rev", revisions.get(itemId));
                }
                result.add(itemResponse);
            }
        }

        return result;
    }

    @Override
    public Map<String, Object> getDocument(String database, String docId) {
        return getDocumentElasticSearch(getElasticSearchIndexNameFromDatabase(database), docId, DOCUMENT_TYPE_DOCUMENT);
    }

    @Override
    public Map<String, Object> getLocalDocument(String database, String docId) {
        return getDocumentElasticSearch(getElasticSearchIndexNameFromDatabase(database), docId, DOCUMENT_TYPE_CHECKPOINT);
    }

    protected Map<String, Object> getDocumentElasticSearch(String index, String docId, String docType) {
        GetResponse response = client.prepareGet(index, docType, docId).execute().actionGet();
        if(response != null && response.exists()) {
            Map<String,Object> esDocument = response.sourceAsMap();
            return (Map<String, Object>)esDocument.get("doc");
        }
        return null;
    }

    @Override
    public String storeDocument(String database, String docId, Map<String, Object> document) {
        return storeDocumentElasticSearch(getElasticSearchIndexNameFromDatabase(database), docId, document, DOCUMENT_TYPE_DOCUMENT);
    }

    @Override
    public String storeLocalDocument(String database, String docId,
            Map<String, Object> document) {
        return storeDocumentElasticSearch(getElasticSearchIndexNameFromDatabase(database), docId, document, DOCUMENT_TYPE_CHECKPOINT);
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
}
