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

import com.couchbase.connector.cluster.consul.ConsulDocumentWatcher;
import com.couchbase.connector.cluster.consul.ConsulHelper;
import com.couchbase.connector.elasticsearch.io.BackoffPolicy;
import com.couchbase.consul.ConsulOps;
import com.couchbase.consul.ConsulResponse;
import com.couchbase.consul.KvReadResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.github.therapi.jsonrpc.client.JsonRpcTransport;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.connector.cluster.consul.ConsulHelper.getWithRetry;
import static com.couchbase.connector.cluster.consul.ReactorHelper.blockSingle;
import static com.couchbase.connector.elasticsearch.io.BackoffPolicyBuilder.constantBackoff;
import static java.util.Objects.requireNonNull;

public class ConsulRpcTransport implements JsonRpcTransport {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsulRpcTransport.class);

  private final String endpointKey;
  private final Duration timeout;

  private final static String requestIdPrefix = UUID.randomUUID() + "#";
  private final static AtomicLong requestCounter = new AtomicLong();
  private final ConsulDocumentWatcher watcher;
  private final ConsulOps.KvOps kv;

  public ConsulRpcTransport(ConsulOps.KvOps kv, ConsulDocumentWatcher watcher, String endpointKey, Duration timeout) {
    this.watcher = requireNonNull(watcher);
    this.endpointKey = requireNonNull(endpointKey);
    this.timeout = requireNonNull(timeout);
    this.kv = requireNonNull(kv);
  }

  public ConsulRpcTransport withTimeout(Duration timeout) {
    return new ConsulRpcTransport(kv, watcher, endpointKey, timeout);
  }

  private String nextRequestId() {
    return requestIdPrefix + requestCounter.getAndIncrement();
  }

  @Override
  public JsonNode execute(ObjectMapper mapper, Object jsonRpcRequest) throws IOException {
    try {
      // retry getting the endpoint document, because the server node might not have created it yet.
      final BackoffPolicy backoffPolicy = constantBackoff(Duration.ofMillis(500)).timeout(timeout).build();
      final ConsulResponse<Optional<KvReadResult>> initialEndpointValue = getWithRetry(kv, endpointKey, backoffPolicy);

      final ObjectNode requestNode = mapper.valueToTree(jsonRpcRequest);
      final JsonNode id = new TextNode(nextRequestId() + "::" + requestNode.path("method").asText());
      requestNode.set("id", id);

      sendRequest(initialEndpointValue, mapper, requestNode);

      final EndpointDocument endpointDocument = watcher
          .awaitCondition(endpointKey,
              json -> readValueUnchecked(mapper, json, EndpointDocument.class),
              endpoint -> endpoint.isEmpty() || endpoint.get().findResponse(id).isPresent(),
              timeout)
          .orElse(null);

      if (endpointDocument == null) {
        throw new IOException("Failed to receive RPC response; endpoint document does not exist: " + endpointKey);
      }

      // expect to find response, since that was the condition we awaited.
      final JsonNode rpcResponse = endpointDocument.findResponse(id)
          .orElseThrow(() -> new AssertionError("Missing rpc response with id " + id));

      removeResponseFromEndpointDocument(mapper, id);
      return rpcResponse;

    } catch (TimeoutException e) {
      throw new IOException("RPC endpoint operation timed out", e);
    } catch (InterruptedException e) {
      throw new InterruptedIOException("RPC endpoint operation interrupted; " + e.getMessage());
    }
  }

  private static <T> T readValueUnchecked(ObjectMapper mapper, String json, Class<T> type) {
    try {
      return mapper.readValue(json, type);
    } catch (IOException e) {
      throw new IllegalArgumentException("Malformed RPC endpoint document", e);
    }
  }

  private void removeResponseFromEndpointDocument(ObjectMapper mapper, JsonNode id) throws IOException {
    final ConsulResponse<Optional<KvReadResult>> initialEndpointValue =
        blockSingle(kv.readOneKey(endpointKey));

    if (initialEndpointValue.body().isEmpty()) {
      return;
    }

    ConsulHelper.atomicUpdate(kv, endpointKey, initialEndpointValue, value -> {
      try {
        if (Strings.isNullOrEmpty(value)) {
          return value;
        }

        EndpointDocument doc = mapper.readValue(value, EndpointDocument.class);
        doc.removeResponse(id);
        return mapper.writeValueAsString(doc);

      } catch (IOException e) {
        LOGGER.error("Failed to remove response with ID {} from malformed RPC endpoint document,", id, e);
        return value;
      }
    });
  }

  private void sendRequest(ConsulResponse<Optional<KvReadResult>> initialEndpointValue, ObjectMapper mapper, ObjectNode requestNode) throws IOException {
    ConsulHelper.atomicUpdate(kv, endpointKey, initialEndpointValue, document -> {
      try {
        final EndpointDocument endpoint = mapper.readValue(document, EndpointDocument.class);
        endpoint.addRequest(requestNode);
        return mapper.writeValueAsString(endpoint);

      } catch (IOException e) {
        throw new IllegalArgumentException("Malformed RPC endpoint document", e);
      }
    });
  }

  @Override
  public String toString() {
    return "ConsulRpcTransport{" +
        "endpointKey='" + redactSystem(endpointKey) + '\'' +
        ", timeout=" + timeout +
        '}';
  }
}
