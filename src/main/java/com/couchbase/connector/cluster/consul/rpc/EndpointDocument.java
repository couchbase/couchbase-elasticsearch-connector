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

package com.couchbase.connector.cluster.consul.rpc;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class EndpointDocument {
  private static final String TIMESTAMP = "timestamp";

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

  /**
   * Removes all responses older than the given duration.
   * <p>
   * Must only be called by the server, since it relies on the server's clock to calculate expiration time
   * (System.nanoTime() is compared against the value recorded by {@link #respond(ObjectNode)}).
   *
   * @return the removed responses
   */
  public List<ObjectNode> removeResponsesOlderThan(Duration expiry) {
    // Using nanoTime here is safe because Consul deletes the whole endpoint document
    // when the server process dies; we won't be comparing values from different processes.
    final long now = System.nanoTime();
    final long expiryNanos = expiry.toNanos();
    final List<ObjectNode> unclaimedResponses = new ArrayList<>();

    for (Iterator<ObjectNode> i = responses.iterator(); i.hasNext(); ) {
      final ObjectNode node = i.next();
      final boolean expired = now - node.path(TIMESTAMP).longValue() > expiryNanos;
      if (expired) {
        unclaimedResponses.add(node);
        i.remove();
      }
    }

    return unclaimedResponses;
  }

  private static ObjectNode checkHasId(ObjectNode node) {
    final JsonNode id = node.get("id");
    if (id == null) {
      throw new IllegalArgumentException("JSON-RPC node is missing 'id': " + node);
    }
    checkIsValidJsonRpcId(id);
    return node;
  }

  private static void checkIsValidJsonRpcId(JsonNode id) {
    checkArgument(id.isTextual() || id.isNumber() || id.isNull(),
        "JSON-RPC ID must be String, Number, or NULL");
  }

  public void addRequest(ObjectNode request) {
    requests.add(checkHasId(request));
  }

  public void respond(ObjectNode response) {
    final JsonNode id = checkHasId(response).get("id");
    response.set(TIMESTAMP, new LongNode(System.nanoTime()));
    responses.add(response);
    requests.removeIf(request -> request.path("id").equals(id));
  }

  public Optional<ObjectNode> firstRequest() {
    return requests.stream().findFirst();
  }

  public Optional<ObjectNode> findResponse(JsonNode id) {
    checkIsValidJsonRpcId(id);
    return responses.stream()
        .filter(r -> r.path("id").equals(id))
        .findFirst();
  }

  public boolean removeResponse(JsonNode id) {
    checkIsValidJsonRpcId(id);
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
