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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableMap;

import com.couchbase.capi.CouchbaseBehavior;

public class ElasticSearchCouchbaseBehavior implements CouchbaseBehavior {

    protected Client client;

    public ElasticSearchCouchbaseBehavior(Client client) {
        this.client = client;
    }

    @Override
    public List<String> getPools() {
        List<String> result = new ArrayList<String>();
        result.add("default");
        return result;
    }

    @Override
    public String getPoolUUID(String pool) {
        ClusterStateRequestBuilder builder = client.admin().cluster().prepareState();
        ClusterStateResponse response = builder.execute().actionGet();
        ClusterName name = response.getClusterName();
        return UUID.nameUUIDFromBytes(name.toString().getBytes()).toString().replace("-", "");
    }

    @Override
    public Map<String, Object> getPoolDetails(String pool) {
        if("default".equals(pool)) {
            Map<String, Object> bucket = new HashMap<String, Object>();
            bucket.put("uri", "/pools/" + pool + "/buckets?uuid=" + getPoolUUID(pool));

            Map<String, Object> responseMap = new HashMap<String, Object>();
            responseMap.put("buckets", bucket);

            List<Object> nodes = getNodesServingPool(pool);
            responseMap.put("nodes", nodes);

            return responseMap;
        }
        return null;
    }

    @Override
    public List<String> getBucketsInPool(String pool) {
        if("default".equals(pool)) {
            List<String> bucketNameList = new ArrayList<String>();

            ClusterStateRequestBuilder stateBuilder = client.admin().cluster().prepareState();
            ClusterStateResponse response = stateBuilder.execute().actionGet();
            ImmutableMap<String, IndexMetaData> indices = response.getState().getMetaData().getIndices();
            for (String index : indices.keySet()) {
                bucketNameList.add(index);
            }

            return bucketNameList;
        }
        return null;
    }

    @Override
    public String getBucketUUID(String pool, String bucket) {
        IndicesExistsRequestBuilder existsBuilder = client.admin().indices().prepareExists(bucket);
        IndicesExistsResponse response = existsBuilder.execute().actionGet();
        if(response.exists()) {
            return "00000000000000000000000000000000";
        }
        return null;
    }

    @Override
    public List<Object> getNodesServingPool(String pool) {
        if("default".equals(pool)) {

            NodesInfoRequestBuilder infoBuilder = client.admin().cluster().prepareNodesInfo((String[]) null);
            NodesInfoResponse infoResponse = infoBuilder.execute().actionGet();

            // extract what we need from this response
            List<Object> nodes = new ArrayList<Object>();
            for (NodeInfo nodeInfo : infoResponse.getNodes()) {

                // FIXME there has to be a better way than
                // parsing this string
                // but so far I have not found it
                if (nodeInfo.serviceAttributes() != null) {
                    for (Map.Entry<String, String> nodeAttribute : nodeInfo
                            .serviceAttributes().entrySet()) {
                        if (nodeAttribute.getKey().equals(
                                "couchbase_address")) {
                            int start = nodeAttribute
                                    .getValue()
                                    .lastIndexOf("/");
                            int end = nodeAttribute
                                    .getValue()
                                    .lastIndexOf("]");
                            String hostPort = nodeAttribute
                                    .getValue().substring(
                                            start + 1, end);
                            String[] parts = hostPort.split(":");

                            Map<String, Object> nodePorts = new HashMap<String, Object>();
                            nodePorts.put("direct", Integer.parseInt(parts[1]));

                            Map<String, Object> node = new HashMap<String, Object>();
                            node.put("couchApiBase", String.format("http://%s/", hostPort));
                            node.put("hostname", hostPort);
                            node.put("ports", nodePorts);

                            nodes.add(node);
                        }
                    }
                }
            }
            return nodes;

        }
        return null;
    }

}
