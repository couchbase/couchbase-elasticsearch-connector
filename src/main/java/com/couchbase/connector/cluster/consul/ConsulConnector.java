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
import com.couchbase.connector.util.ThrowableHelper;
import com.github.therapi.core.MethodRegistry;
import com.github.therapi.jsonrpc.DefaultExceptionTranslator;
import com.github.therapi.jsonrpc.JsonRpcDispatcher;
import com.github.therapi.jsonrpc.JsonRpcError;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.orbitz.consul.Consul;
import com.orbitz.consul.model.agent.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import static com.couchbase.connector.cluster.consul.ConsulHelper.endpointId;
import static com.github.therapi.jackson.ObjectMappers.newLenientObjectMapper;
import static com.google.common.base.Preconditions.checkState;

public class ConsulConnector {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsulConnector.class);

  public static void main(String[] args) throws Exception {
    final String serviceName = args[0];
    final String serviceIdOrNull = args.length == 1 ? null : args[1];
    run(new ConsulContext(Consul.builder(), serviceName, serviceIdOrNull));
  }

  public static void run(ConsulContext ctx) throws Exception {
    final BlockingQueue<Throwable> fatalErrorQueue = new LinkedBlockingQueue<>();

    final Consumer<Throwable> errorConsumer = e -> {
      LOGGER.error("Got fatal error", e);
      fatalErrorQueue.add(e);
    };

    final Consul consul = ctx.consul();
    Thread shutdownHook = null;

    final Member member = consul.agentClient().getAgent().getMember();
    final String endpointId = endpointId(member, ctx.serviceId());

    final List<AbstractLongPollTask> waitForMe = new ArrayList<>();

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

      waitForMe.add(election);
      waitForMe.add(rpc);

      shutdownHook = new Thread(() -> {
        try {
          LOGGER.info("Shutting down");

          session.close(); // to prevent race with updating (failing) the health check
          election.close();
          rpc.close();
          consul.destroy();
          election.awaitTermination();
          rpc.awaitTermination();

          reportTermination(ctx, null);

        } catch (Exception e) {
          System.err.println("Exception in connector shutdown hook.");
          e.printStackTrace();
        }
      });
      Runtime.getRuntime().addShutdownHook(shutdownHook);

      fatalError = fatalErrorQueue.take();

    } catch (Throwable t) {
      fatalError = t;

    } finally {
      workerService.close();

      if (shutdownHook != null) {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
      }

      // Cancel any outstanding I/O operations such as the long polls for leader election;
      // see https://github.com/rickfast/consul-client/issues/307
      consul.destroy();

      for (AbstractLongPollTask task : waitForMe) {
        task.awaitTermination();
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

    System.exit(1);
  }

  private static void reportTermination(ConsulContext ctx, Throwable cause) {
    ctx.runCleanup(tempConsul -> {
      try {
        final StringBuilder message = new StringBuilder("(" + ctx.serviceId() + ") Graceful shutdown complete.");
        if (cause != null) {
          message.append(" Shutdown triggered by exception: ").append(Throwables.getStackTraceAsString(cause));
        }
        tempConsul.agentClient().fail(ctx.serviceId(), message.toString());

      } catch (Throwable t) {
        System.err.println("Failed to report termination to Consul agent.");
        t.printStackTrace();
      }
    });
  }
}
