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

import com.couchbase.connector.cluster.consul.rpc.RpcServerTask;
import com.couchbase.connector.util.RuntimeHelper;
import com.couchbase.connector.util.ThrowableHelper;
import com.github.therapi.core.MethodRegistry;
import com.github.therapi.jsonrpc.DefaultExceptionTranslator;
import com.github.therapi.jsonrpc.JsonRpcDispatcher;
import com.github.therapi.jsonrpc.JsonRpcError;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import static com.github.therapi.jackson.ObjectMappers.newLenientObjectMapper;
import static com.google.common.base.Preconditions.checkState;

public class ConsulConnector {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsulConnector.class);

  public static void run(ConsulContext ctx) throws Exception {
    final BlockingQueue<Throwable> fatalErrorQueue = new LinkedBlockingQueue<>();

    final Consumer<Throwable> errorConsumer = e -> {
      LOGGER.error("Got fatal error", e);
      fatalErrorQueue.add(e);
    };

    Thread shutdownHook = null;

    final String endpointId = ctx.myEndpointId();
    LOGGER.info("Endpoint ID: {}", endpointId);

    final WorkerServiceImpl workerService = new WorkerServiceImpl(e -> {
      LOGGER.error("Got fatal error", e);

      if (e instanceof java.net.BindException) {
        System.err.println();
        System.err.println("ERROR: " + e);
        System.err.println();
        System.err.println("  This may occur if multiple connector workers running on the same host");
        System.err.println("  all try to start an HTTP server on the same port. Try setting the");
        System.err.println("  'httpPort' config option to 0 to use an ephemeral port instead.");
        System.err.println();
      }

      System.exit(1);
    });

    final MethodRegistry methodRegistry = new MethodRegistry(newLenientObjectMapper());
    methodRegistry.scan(workerService);

    LOGGER.info("Registered JSON-RPC methods: {}", methodRegistry.getMethods());

    final JsonRpcDispatcher dispatcher = JsonRpcDispatcher.builder(methodRegistry)
        .exceptionTranslator(new DefaultExceptionTranslator() {
          @Override
          protected JsonRpcError translateCustom(Throwable t) {
            JsonRpcError error = super.translateCustom(t);
            error.setData(ImmutableMap.of("detail", ImmutableMap.of(
                "exception", t.toString(),
                "stackTrace", Throwables.getStackTraceAsString(t))));
            return error;
          }
        })
        .build();

    final LeaderController leaderController = new LeaderController() {
      private volatile LeaderTask leader;

      @Override
      public void startLeading() {
        checkState(leader == null, "Already leading");
        leader = new LeaderTask(ctx).start();
      }

      @Override
      public void stopLeading() {
        if (leader != null) {
          leader.stop();
        }
        leader = null;
      }
    };

    Throwable fatalError = null;

    try (SessionTask session =
             new SessionTask(ctx, workerService::resetKillSwitchTimer, errorConsumer).start();

         RpcServerTask rpc =
             new RpcServerTask(dispatcher, ctx, session.sessionId(), endpointId, errorConsumer).start();

         LeaderElectionTask election =
             new LeaderElectionTask(ctx, session.sessionId(), endpointId, errorConsumer, leaderController).start()) {

      shutdownHook = new Thread(() -> {
        try {
          LOGGER.info("Shutting down");

          // Close session task first to prevent race with updating (failing) the health check.
          // Doesn't invalidate session; just stops heartbeat.
          session.close();

          election.close();
          rpc.close();

          // clean termination -- yay!
          reportTermination(ctx, null);

        } catch (Exception e) {
          System.err.println(Instant.now() + " ERROR: Exception in connector shutdown hook.");
          e.printStackTrace();
        }
      }, "consul-connector-shutdown");
      RuntimeHelper.addShutdownHook(shutdownHook);

      fatalError = fatalErrorQueue.take();

    } finally {
      workerService.close();

      if (shutdownHook != null) {
        RuntimeHelper.removeShutdownHook(shutdownHook);
      }

      reportTermination(ctx, fatalError);
    }

    if (ThrowableHelper.hasCause(fatalError, RpcServerTask.EndpointAlreadyInUseException.class)) {
      System.err.println();
      System.err.println("ERROR: " + fatalError);
      System.err.println();
      System.err.println("  This may occur if this worker previously terminated before it could");
      System.err.println("  gracefully end its Consul session. In that case, wait for the Consul");
      System.err.println("  lock delay to elapse (15 seconds by default) and try again.");
      System.err.println();
      System.err.println("  This can also happen if you're trying to run more than one connector");
      System.err.println("  worker in the same group using the same Consul agent without assigning");
      System.err.println("  each worker a unique Consul service ID. Specify a unique ID with the");
      System.err.println("  --service-id command line option.");
      System.err.println();
    }
  }

  private static void reportTermination(ConsulContext ctx, Throwable cause) {
    ctx.runCleanup(() -> ctx.reportShutdown(cause));
  }
}
