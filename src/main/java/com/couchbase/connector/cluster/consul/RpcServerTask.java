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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.therapi.jsonrpc.JsonRpcDispatcher;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.kv.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Optional;
import java.util.function.Consumer;

import static com.github.therapi.jackson.ObjectMappers.newLenientObjectMapper;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RpcServerTask extends AbstractLongPollTask<RpcServerTask> {

  private static final ObjectMapper mapper = newLenientObjectMapper();
  private static final ObjectWriter objectWriter = mapper.writerWithDefaultPrettyPrinter();

  static class EndpointAlreadyInUseException extends Exception {
    public EndpointAlreadyInUseException(String message) {
      super(message);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(RpcServerTask.class);

  private final Consumer<Throwable> fatalErrorConsumer;
  private final String endpointId;
  private final String endpointKey;
  private final KeyValueClient kv;
  private final String sessionId;
  private final JsonRpcDispatcher dispatcher;

  public RpcServerTask(JsonRpcDispatcher dispatcher, KeyValueClient kv, String serviceName, String sessionId, String endpointId, Consumer<Throwable> fatalErrorConsumer) {
    super(kv, "rpc-server-", serviceName, sessionId);
    this.kv = requireNonNull(kv);
    this.sessionId = requireNonNull(sessionId);
    this.fatalErrorConsumer = requireNonNull(fatalErrorConsumer);
    this.dispatcher = requireNonNull(dispatcher);

    this.endpointId = requireNonNull(endpointId);
    this.endpointKey = "couchbase/cbes/" + serviceName + "/rpc/" + endpointId;
  }

  @Override
  protected void doRun(KeyValueClient kv, String serviceName, String sessionId) {
    try {
      bind();

      BigInteger index = BigInteger.ZERO;

      while (!closed()) {
        final ConsulResponse<Value> response = ConsulHelper.awaitChange(kv, endpointKey, index);
        if (response == null) {
          LOGGER.warn("RPC endpoint was deleted externally; will attempt to rebind");
          bind();
          continue;
        }

        final String json = response.getResponse().getValueAsString(UTF_8).orElse("{}");
        EndpointDocument initialEndpoint = mapper.readValue(json, EndpointDocument.class);

        final ObjectNode jsonRpcRequest = initialEndpoint.firstRequest().orElse(null);
        if (jsonRpcRequest == null) {
          LOGGER.debug("No unanswered requests.");
        } else {
          final ObjectNode invocationResult = execute(jsonRpcRequest);

          ConsulHelper.atomicUpdate(kv, response, document -> {
            try {
              final EndpointDocument endpoint = mapper.readValue(document, EndpointDocument.class);
              endpoint.respond(invocationResult);
              return objectWriter.writeValueAsString(endpoint);

            } catch (IOException e) {
              throw new IllegalArgumentException("Malformed RPC endpoint document", e);
            }
          });

          LOGGER.info("Endpoint update complete");
        }

        index = response.getIndex();
      }

    } catch (Throwable t) {
      if (closed()) {
        // Closing the task is likely to result in an InterruptedException,
        // or a ConsulException wrapping an InterruptedIOException.
        LOGGER.debug("Caught exception in RPC server loop after closing. Don't panic; this is expected.", t);
      } else {
        // Something went horribly, terribly, unrecoverably wrong.
        fatalErrorConsumer.accept(t);
      }

    } finally {
      try {
        // Unbind (delete RPC endpoint document); only succeeds if we own the lock. This lets another node acquire
        // the lock immediately. If we don't do this, the lock will be auto-released when the session ends,
        // but the lock won't be eligible for acquisition until the Consul lock delay has elapsed.

        LOGGER.info("Unbinding from RPC endpoint document {}", endpointKey);
        clearInterrupted(); // so final Consul request doesn't fail with InterruptedException
        ConsulHelper.unlockAndDelete(kv, endpointKey, sessionId);

      } catch (Exception e) {
        LOGGER.warn("Failed to unbinds", e);
      }
    }
  }

  private ObjectNode execute(ObjectNode request) {
    if (!request.has("id")) {
      throw new IllegalArgumentException("JSON-RPC request node is missing 'id' (notifications not supported)");
    }

    return (ObjectNode) dispatcher.invoke(request.toString()).orElseThrow(() -> new AssertionError("missing response"));
  }

  private void bind() throws InterruptedException, EndpointAlreadyInUseException {
    while (!closed()) {
      LOGGER.info("Attempting to binding to RPC endpoint document {}", endpointKey);

      final boolean acquired = kv.acquireLock(endpointKey, "{}", sessionId);
      if (acquired) {
        LOGGER.info("Successfully bound to RPC endpoint document {}", endpointKey);
        return;
      }

      final Optional<ConsulResponse<Value>> existing = kv.getConsulResponseWithValue(endpointKey);
      if (existing.isPresent()) {
        // somebody else has acquired the lock
        final String lockSession = existing.get().getResponse().getSession().orElse(null);
        throw new EndpointAlreadyInUseException(
            "Failed to lock RPC endpoint document " + endpointKey + " ; already locked by session " + lockSession);
      }

      LOGGER.info("Endpoint lock acquisition failed; will retry after delay.");
      SECONDS.sleep(1);
    }
  }
}
