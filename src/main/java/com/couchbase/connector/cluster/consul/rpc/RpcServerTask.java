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

import com.couchbase.client.core.util.CbThrowables;
import com.couchbase.connector.cluster.consul.AbstractLongPollTask;
import com.couchbase.connector.cluster.consul.ConsulContext;
import com.couchbase.connector.cluster.consul.ConsulHelper;
import com.couchbase.consul.ConsulResponse;
import com.couchbase.consul.KvReadResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.therapi.jsonrpc.JsonRpcDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.couchbase.connector.cluster.consul.ConsulHelper.requireModifyIndex;
import static com.couchbase.connector.cluster.consul.ReactorHelper.blockSingle;
import static com.github.therapi.jackson.ObjectMappers.newLenientObjectMapper;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RpcServerTask extends AbstractLongPollTask<RpcServerTask> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RpcServerTask.class);
  private static final Duration unclaimedResponseTtl = Duration.ofMinutes(5);

  private static final ObjectMapper mapper = newLenientObjectMapper();
  private static final ObjectWriter objectWriter = mapper.writerWithDefaultPrettyPrinter();

  public static class EndpointAlreadyInUseException extends Exception {
    public EndpointAlreadyInUseException(String message) {
      super(message);
    }
  }

  private final Consumer<Throwable> fatalErrorConsumer;
  private final String endpointId;
  private final String endpointKey;
  private final String sessionId;
  private final JsonRpcDispatcher dispatcher;
  private final ConsulContext ctx;

  public RpcServerTask(JsonRpcDispatcher dispatcher, ConsulContext ctx, String sessionId, String endpointId, Consumer<Throwable> fatalErrorConsumer) {
    super(ctx, "rpc-server-", sessionId);
    this.sessionId = requireNonNull(sessionId);
    this.fatalErrorConsumer = requireNonNull(fatalErrorConsumer);
    this.dispatcher = requireNonNull(dispatcher);

    this.endpointId = requireNonNull(endpointId);
    this.endpointKey = ctx.keys().rpcEndpoint(endpointId);
    this.ctx = requireNonNull(ctx);
  }

  @Override
  protected void doRun(ConsulContext ctx, String sessionId) {
    try {
      bind();
      long index = 0;

      while (!closed()) {
        final ConsulResponse<Optional<KvReadResult>> response = ConsulHelper.awaitChange(ctx.client().kv(), endpointKey, index);
        if (response.body().isEmpty()) {
          LOGGER.warn("RPC endpoint was deleted externally; will attempt to rebind");
          bind();
          index = 0;
          continue;
        }

        final String json = response.body().get().valueAsString();
        final EndpointDocument initialEndpoint = mapper.readValue(json, EndpointDocument.class);

        final ObjectNode jsonRpcRequest = initialEndpoint.firstRequest().orElse(null);
        if (jsonRpcRequest == null) {
          LOGGER.debug("No unanswered requests.");
        } else {
          final ObjectNode invocationResult = execute(jsonRpcRequest);

          ConsulHelper.atomicUpdate(ctx.client().kv(), endpointKey, response, document -> {
            try {
              final EndpointDocument endpoint = mapper.readValue(document, EndpointDocument.class);

              final List<ObjectNode> unclaimedResponses = endpoint.removeResponsesOlderThan(unclaimedResponseTtl);
              if (!unclaimedResponses.isEmpty()) {
                LOGGER.warn("Removed unclaimed responses (expired): {}", unclaimedResponses);
              }

              endpoint.respond(invocationResult);
              return objectWriter.writeValueAsString(endpoint);

            } catch (IOException e) {
              throw new IllegalArgumentException("Malformed RPC endpoint document", e);
            }
          });

          LOGGER.debug("Endpoint update complete");
        }

        index = requireModifyIndex(response);
      }

    } catch (Throwable t) {
      if (closed()) {
        if (CbThrowables.hasCause(t, InterruptedException.class)
            || CbThrowables.hasCause(t, InterruptedIOException.class)) {
          // Closing the task is likely to result in an InterruptedException
          LOGGER.debug("RPC server loop closed due to interruption. Don't panic; this is expected.");
        } else {
          LOGGER.warn("Caught unexpected exception in RPC server loop after closing." +
              " It's probably nothing to worry about, but logging it just in case.", t);
        }
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
        ctx.runCleanup(() -> ctx.unlockAndDelete(endpointKey, sessionId));

      } catch (Exception e) {
        LOGGER.warn("Failed to unbind", e);
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

      final boolean acquired = ctx.acquireLock(endpointKey, "{}", sessionId);
      if (acquired) {
        LOGGER.info("Successfully bound to RPC endpoint document {}", endpointKey);
        return;
      }

      final ConsulResponse<Optional<KvReadResult>> existing = blockSingle(ctx.client().kv().readOneKey(endpointKey, Map.of()));
      if (existing.body().isPresent()) {
        // somebody else has acquired the lock
        final String lockSession = existing.body().get().session().orElse(null);
        throw new EndpointAlreadyInUseException(
            "Failed to lock RPC endpoint document " + endpointKey + " ; already locked by session " + lockSession);
      }

      LOGGER.info("Endpoint lock acquisition failed; will retry after delay.");
      SECONDS.sleep(1);
    }
  }
}
