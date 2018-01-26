/*
 * Copyright 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.transport.couchbase.capi;

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Maps.newHashMapWithExpectedSize;

/**
 * In Elasticsearch, a child documents must live on the same shard as its parent
 * (they must have the same "routing"). In order to delete a child document,
 * you must specify the routing so the deletion request goes to the node where the child lives.
 * If the parent ID comes from a field of the child document, then we have a problem:
 * Couchbase does not send the body of deleted documents, so we don't
 * know who the parent is, and we don't have enough info to route the request correctly.
 * <p>
 * The DeletionRouter addresses this problem by executing Elasticsearch requests
 * to find the correct routing.
 * <p>
 * When a child document is created, DeletionRouter saves a shadow document called a
 * "routing signpost" that contains the routing value used when the document was created.
 * When the child document is deleted, the DeletionRouter reads the signpost document
 * and uses sets the appropriate routing on the deletion request. It then deletes the
 * signpost as well.
 */
class DeletionRouter {
    private static final Logger logger = Loggers.getLogger(DeletionRouter.class);

    private static final String ROUTING_SINGPOST_TYPE = "couchbaseSignpost";
    private static final String ROUTING_SINGPOST_ID_PREFIX = ROUTING_SINGPOST_TYPE + "::";
    private static final int ROUTING_SINGPOST_ID_PREFIX_LENGTH = ROUTING_SINGPOST_ID_PREFIX.length();

    private final Client client;
    private final String index;
    private final PluginSettings pluginSettings;
    private final Timer queryTimer;
    private final List<String> deletionIdsThatRequireRouting = new ArrayList<>();

    DeletionRouter(Client client, String index, PluginSettings pluginSettings, Timer queryTimer) {
        this.client = client;
        this.index = index;
        this.pluginSettings = pluginSettings;
        this.queryTimer = queryTimer;
    }

    void addRoutingLater(String deletedDocumentId) {
        deletionIdsThatRequireRouting.add(deletedDocumentId);
    }

    void augmentBulkRequests(Map<String, IndexRequest> idToIndexRequest,
                             Map<String, DeleteRequest> idToDeleteRequest) {
        // For each saved document, create a "routing signposts" that records the routing info.
        createRoutingSignpostDocuments(idToIndexRequest);

        // When deleting a document, look for a routing signpost and use it to supply the correct routing.
        // Deletes the routing signpost too.
        addRoutingToDeletionRequests(idToIndexRequest, idToDeleteRequest);
    }

    /**
     * For document with non-default routing, also persist a separate "signpost" document
     * containing the routing information required for locating & deleting the document later.
     * This signpost document itself uses default routing so it can be easily located.
     * <p>
     * CAVEAT: The signpost may live on a different ES shard than the document it refers to.
     * A failure of the the signpost's shard will make it impossible for the plugin
     * to delete the document, because it won't know where to route the deletion request.
     */
    private void createRoutingSignpostDocuments(Map<String, IndexRequest> idToIndexRequest) {
        List<IndexRequest> signpostIndexRequests = new ArrayList<>();

        for (IndexRequest indexRequest : idToIndexRequest.values()) {
            final String routing = getRoutingOrParent(indexRequest);
            if (routing == null) {
                continue;
            }

            final String routingSignpostId = addRoutingSignpostIdPrefix(indexRequest.id());
            final IndexRequest routingSignpostIndexRequest =
                    client.prepareIndex(index, routingSignpostType(), routingSignpostId)
                            .setSource(newRoutingSignpostSource(routing))
                            .setTTL(indexRequest.ttl())
                            .request();

            signpostIndexRequests.add(routingSignpostIndexRequest);
        }

        // Do this is a separate loop to avoid ConcurrentModificationException when iterating over map values
        for (IndexRequest indexRequest : signpostIndexRequests) {
            idToIndexRequest.put(indexRequest.id(), indexRequest);
        }
    }

    private static String getRoutingOrParent(IndexRequest req) {
        return req.routing() != null ? req.routing() : req.parent();
    }

    /**
     * Supplies routing info for deletion requests that require special routing.
     * Also deletes the routing signpost.
     *
     * @param bulkIndexRequests The indexing requests created so far, keyed by document ID. Used to get routing info
     * for documents that are created and deleted in the same CAPI payload.
     * @param bulkDeleteRequests The deletion requests created so far, keyed by document ID. The method may add
     * additional deletion requests to this map.
     */
    private void addRoutingToDeletionRequests(Map<String, IndexRequest> bulkIndexRequests,
                                              Map<String, DeleteRequest> bulkDeleteRequests) {
        if (deletionIdsThatRequireRouting.isEmpty()) {
            return;
        }

        final Map<String, String> idToRouting = getRoutingFromSignpostsWithRetry(deletionIdsThatRequireRouting);
        for (String id : deletionIdsThatRequireRouting) {
            String routing = idToRouting.get(id);
            if (routing == null) {
                // No signpost exists for this document.
                // The document may have been created and deleted in the same batch. Handle it.
                IndexRequest indexRequest = bulkIndexRequests.get(id);
                if (indexRequest != null && indexRequest.parent() != null) {
                    routing = indexRequest.parent();
                } else {
                    logger.warn("Missing routing signpost. Unable to delete document: {}", id);
                    bulkDeleteRequests.remove(id);
                    continue;
                }
            }

            logger.trace("Will delete document '{}' using routing '{}'", id, routing);
            DeleteRequest deleteRequest = bulkDeleteRequests.get(id);
            if (deleteRequest == null) {
                logger.warn("Lost track of delete request. Unable to delete document: {}", id);
                continue;
            }
            deleteRequest.routing(routing);

            // Delete the signpost too
            String routingSignpostId = addRoutingSignpostIdPrefix(id);
            bulkDeleteRequests.put(routingSignpostId,
                    client.prepareDelete(index, routingSignpostType(), routingSignpostId).request());
        }
    }

    /**
     * Given a list of document ids, looks up the associated signposts and returns a map from document ID to routing.
     * Document IDs for which there are no signposts will not be included in the returned map.
     */
    private Map<String, String> getRoutingFromSignpostsWithRetry(Collection<String> ids) {
        final long maxRetries = pluginSettings.getBulkIndexRetries();
        final long retryWaitMillis = pluginSettings.getBulkIndexRetryWaitMs();
        final boolean ignoreFailures = pluginSettings.getIgnoreFailures();

        final Map<String, String> idToRouting = newHashMapWithExpectedSize(ids.size());

        List<MultiGetResponse.Failure> failures = populateIdToRoutingMapAndReturnFailures(ids, idToRouting);
        if (failures.isEmpty()) {
            return idToRouting;
        }

        long retriesLeft = maxRetries;

        while (!failures.isEmpty()) {
            if (retriesLeft-- <= 0) {
                final String message = "Failed to get routing signposts after " + maxRetries + " retries";
                final Throwable cause = failures.get(0).getFailure();

                if (!ignoreFailures) {
                    throw new RuntimeException(message, cause);
                }

                logger.warn("Ignoring failures: {}", message, cause);
                break;
            }

            final List<String> idsToRetry = getIds(failures);
            logger.warn("Sleeping for {} ms then retrying routing signpost lookup due to multi-get failure for {} documents including {}",
                    retryWaitMillis, idsToRetry.size(), idsToRetry.get(0));

            logger.debug("Exception that triggered retry:", failures.get(0).getFailure());

            try {
                Thread.sleep(retryWaitMillis);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            failures = populateIdToRoutingMapAndReturnFailures(idsToRetry, idToRouting);
        }

        return idToRouting;
    }

    private List<MultiGetResponse.Failure> populateIdToRoutingMapAndReturnFailures(Collection<String> ids,
                                                                                   Map<String, String> idToRouting) {
        final List<MultiGetResponse.Failure> failures = new ArrayList<>(0);

        // PERFORMANCE CAVEAT:
        //
        // If any of the signpost documents have not yet been indexed, executing this GET request will trigger
        // a refresh, causing *all* new documents to be indexed. This will happen when the same document
        // is both created and deleted within the ES refresh interval (one second by default).
        // It's fine if this happens every now and then, but constant refreshes can lead to poor ES performance.
        final MultiGetRequestBuilder bulkRequest = client.prepareMultiGet();

        for (String id : ids) {
            String signpostId = addRoutingSignpostIdPrefix(id);
            bulkRequest.add(index, routingSignpostType(), signpostId);
        }

        Timer.Context timerContext = queryTimer.time();
        MultiGetResponse multiGetResponse;
        try {
            multiGetResponse = bulkRequest.execute().actionGet();
        } catch (Exception e) {
            logger.debug("Routing signpost multi-get failed", e);
            for (String id : ids) {
                failures.add(new MultiGetResponse.Failure(index, routingSignpostType(), id, e));
            }
            return failures;
        } finally {
            long elapsedNanos = timerContext.stop();
            if (logger.isDebugEnabled()) {
                logger.debug("Multi-get for {} routing signposts finished in {} ms",
                        ids.size(), TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
            }
        }

        for (MultiGetItemResponse item : multiGetResponse) {
            if (item.isFailed()) {
                MultiGetResponse.Failure failure = item.getFailure();
                failures.add(failure);
                logger.debug("Failure getting {} : {}", item.getId(), failure.getMessage(), failure.getFailure());
                continue;
            }

            GetResponse response = item.getResponse();
            if (!response.isExists()) {
                logger.debug("Routing signpost document does not exist: {}", item.getId());
                continue;
            }

            Map<String, Object> source = response.getSource();
            if (source == null) {
                logger.warn("Routing signpost document source is null: {}", item.getId());
                continue;
            }

            String targetDocumentId = removeRoutingSignpostIdPrefix(item.getId());
            String routing = getRoutingFromSignpostDocumentSource(source);
            idToRouting.put(targetDocumentId, routing);
        }

        logger.trace("ID to routing (from signposts): {}", idToRouting);

        return failures;
    }

    private static List<String> getIds(Collection<MultiGetResponse.Failure> failures) {
        List<String> ids = new ArrayList<>(failures.size());
        for (MultiGetResponse.Failure f : failures) {
            ids.add(f.getId());
        }
        return ids;
    }

    private static Map<String, Object> newRoutingSignpostSource(String routing) {
        // Use "meta" as the top level field because the default Couchbase type mapping
        // marks it as "stored". It's important for the field to be stored because we need to read it later.
        // We could tell users to add a specific type mapping for the routing signpost type, but that
        // doesn't seem necessary.
        return ImmutableMap.of("meta", ImmutableMap.of("routing", routing));
    }

    @SuppressWarnings("unchecked")
    private static String getRoutingFromSignpostDocumentSource(Map<String, Object> signpostSource) {
        Map<String, Object> meta = (Map<String, Object>) signpostSource.get("meta");
        if (meta == null) {
            throw new RuntimeException("Routing signpost document source is missing 'meta' field. " +
                    "Make sure the mapping for document type '" + routingSignpostType() + "' stores this field.");
        }
        return (String) meta.get("routing");
    }

    private static String removeRoutingSignpostIdPrefix(String signpostId) {
        return signpostId.substring(ROUTING_SINGPOST_ID_PREFIX_LENGTH);
    }

    private static String addRoutingSignpostIdPrefix(String documentId) {
        return ROUTING_SINGPOST_ID_PREFIX + documentId;
    }

    private static String routingSignpostType() {
        return ROUTING_SINGPOST_TYPE;
    }
}
