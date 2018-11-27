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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.github.therapi.jsonrpc.client.JsonRpcHttpClient;
import com.google.common.base.Strings;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.kv.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.couchbase.connector.cluster.consul.ConsulHelper.awaitCondition;
import static com.couchbase.connector.cluster.consul.ConsulHelper.rpcEndpointKey;
import static java.util.Objects.requireNonNull;

public class ConsulRpcTransport implements JsonRpcHttpClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsulRpcTransport.class);

  private final KeyValueClient kv;
  private final String endpointKey;

  public ConsulRpcTransport(KeyValueClient kv, String serviceName, String endpointId) {
    this.kv = requireNonNull(kv);
    this.endpointKey = rpcEndpointKey(serviceName, endpointId);
  }

  @Override
  public JsonNode execute(ObjectMapper mapper, Object jsonRpcRequest) throws IOException {
    final ConsulResponse<Value> initialEndpointValue = kv.getConsulResponseWithValue(endpointKey).orElse(null);
    if (initialEndpointValue == null) {
      // Server has not yet bound to endpoint
      // todo could wait a bit and retry (analogous to a socket connection timeout)
      throw new IOException("Failed to send RPC request; endpoint document does not exist: " + endpointKey);
    }

    final ObjectNode requestNode = mapper.valueToTree(jsonRpcRequest);
    final JsonNode id = new TextNode(UUID.randomUUID().toString());
    requestNode.set("id", id);

    sendRequest(initialEndpointValue, mapper, requestNode);

    final Function<String, EndpointDocument> parseEndpoint = readValueUnchecked(mapper, EndpointDocument.class);
    final EndpointDocument endpointDocument = awaitCondition(kv, endpointKey, parseEndpoint, hasResponseWithId(id));

    if (endpointDocument == null) {
      throw new IOException("Failed to receive RPC response; endpoint document does not exist: " + endpointKey);
    }

    final JsonNode rpcResponse = endpointDocument.findResponse(id).orElseThrow(() -> new AssertionError("Missing rpc response with id " + id));
    removeResponseFromEndpointDocument(mapper, id);
    return rpcResponse;
  }

  private static <T> Function<String, T> readValueUnchecked(ObjectMapper mapper, Class<T> type) {
    return json -> {
      try {
        return mapper.readValue(json, type);
      } catch (IOException e) {
        throw new IllegalArgumentException("Malformed RPC endpoint document", e);
      }
    };
  }

  private void removeResponseFromEndpointDocument(ObjectMapper mapper, JsonNode id) {
    final ConsulResponse<Value> initialEndpointValue = kv.getConsulResponseWithValue(endpointKey).orElse(null);
    if (initialEndpointValue == null) {
      return;
    }

    ConsulHelper.atomicUpdate(kv, initialEndpointValue, value -> {
      try {
        if (Strings.isNullOrEmpty(value)) {
          return value;
        }

        EndpointDocument doc = mapper.readValue(value, EndpointDocument.class);
        doc.removeResponse(id);
        return mapper.writeValueAsString(doc);

      } catch (IOException e) {
        LOGGER.error("Failed to remove response with ID {} from malformed RPC endpoint document,", e);
        return value;
      }
    });
  }

  private Predicate<EndpointDocument> hasResponseWithId(JsonNode id) {
    return doc -> doc.findResponse(id).isPresent();
  }

  private void sendRequest(ConsulResponse<Value> initialEndpointValue, ObjectMapper mapper, ObjectNode requestNode) {
    // todo add a timeout!!!
    ConsulHelper.atomicUpdate(kv, initialEndpointValue, document -> {
      try {
        final EndpointDocument endpoint = mapper.readValue(document, EndpointDocument.class);
        endpoint.addRequest(requestNode);
        return mapper.writeValueAsString(endpoint);

      } catch (IOException e) {
        throw new IllegalArgumentException("Malformed RPC endpoint document", e);
      }
    });
  }
}
