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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class EndpointDocument {
  private final List<ObjectNode> requests;
  private final List<ObjectNode> responses;

  public EndpointDocument(@JsonProperty("requests") List<ObjectNode> requests,
                          @JsonProperty("responses") List<ObjectNode> responses) {
    this.requests = requests != null ? requests : new ArrayList<>();
    this.responses = responses != null ? responses : new ArrayList<>();
  }

  public List<ObjectNode> getRequests() {
    return requests;
  }

  public List<ObjectNode> getResponses() {
    return responses;
  }

  private static ObjectNode checkHasId(ObjectNode node) {
    final JsonNode id = node.get("id");
    if (id == null) {
      throw new IllegalArgumentException("JSON-RPC node is missing 'id': " + node);
    }
    return node;
  }

  public void addRequest(ObjectNode request) {
    requests.add(checkHasId(request));
  }

  public void respond(ObjectNode response) {
    final JsonNode id = checkHasId(response).get("id");
    responses.add(response);
    requests.removeIf(request -> request.path("id").equals(id));
  }

  public Optional<ObjectNode> firstRequest() {
    return requests.isEmpty() ? Optional.empty() : Optional.of(requests.get(0));
  }

  public Optional<ObjectNode> findResponse(JsonNode id) {
    return responses.stream()
        .filter(r -> r.path("id").equals(id))
        .findFirst();
  }

  public boolean removeResponse(JsonNode id) {
    return responses.removeIf(r -> r.path("id").equals(id));
  }

  @Override
  public String toString() {
    return "EndpointDocument{" +
        "requests=" + requests +
        ", responses=" + responses +
        '}';
  }
}
