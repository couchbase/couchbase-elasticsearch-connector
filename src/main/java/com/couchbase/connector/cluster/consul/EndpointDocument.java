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

package com.couchbase.connector.cluster.consul;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class EndpointDocument {
  private final List<ObjectNode> requests;
  private final List<ObjectNode> responses;

  public EndpointDocument(@JsonProperty("requests") List<ObjectNode> requests, @JsonProperty("responses") List<ObjectNode> responses) {
    this.requests = requests != null ? requests : new ArrayList<>();
    this.responses = responses != null ? responses : new ArrayList<>();
  }

  public List<ObjectNode> getRequests() {
    return requests;
  }

  public List<ObjectNode> getResponses() {
    return responses;
  }

  public void addResponse(ObjectNode response) {
    responses.add(response);
  }

  @JsonIgnore
  public List<ObjectNode> getUnansweredRequests() {
    final Map<JsonNode, ObjectNode> idToRequest = RpcServerTask.indexById(getRequests());
    final Map<JsonNode, ObjectNode> idToResponse = RpcServerTask.indexById(getResponses());

    final Map<JsonNode, ObjectNode> idToUnansweredRequests = new LinkedHashMap<>(idToRequest);
    idToUnansweredRequests.keySet().removeAll(idToResponse.keySet());
    return new ArrayList<>(idToUnansweredRequests.values());
  }

  public Optional<ObjectNode> firstUnansweredRequest() {
    final LinkedHashMap<JsonNode, ObjectNode> idToRequest = RpcServerTask.indexById(getRequests());
    final LinkedHashMap<JsonNode, ObjectNode> idToResponse = RpcServerTask.indexById(getResponses());

    for (Map.Entry<JsonNode, ObjectNode> e : idToRequest.entrySet()) {
      if (!idToResponse.containsKey(e.getKey())) {
        return Optional.of(e.getValue());
      }
    }
    return Optional.empty();
  }

  @Override
  public String toString() {
    return "EndpointDocument{" +
        "requests=" + requests +
        ", responses=" + responses +
        '}';
  }
}
