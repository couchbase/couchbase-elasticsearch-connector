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

import static com.github.therapi.jackson.ObjectMappers.newLenientObjectMapper;
import static com.google.common.base.Preconditions.checkState;

public class Sandbox {
  private static final Logger LOGGER = LoggerFactory.getLogger(Sandbox.class);
  public static final String serviceName = "service-registration-test";

  private static String endpointId(Member member, String serviceId) {
    return member.getName() + "::" + member.getAddress() + "::" + serviceId;
  }

  public static void main(String[] args) throws Exception {
    final String serviceId = args.length == 0 ? Sandbox.serviceName : args[0];
    final BlockingQueue<Throwable> fatalErrorQueue = new LinkedBlockingQueue<>();

    final Consumer<Throwable> errorConsumer = e -> {
      LOGGER.error("Got fatal error", e);
      fatalErrorQueue.add(e);
    };

    final Consul consul = Consul.newClient();
    final DocumentKeys documentKeys = new DocumentKeys(consul.keyValueClient(), serviceName);
    Thread shutdownHook = null;

    final Member member = consul.agentClient().getAgent().getMember();
    final String endpointId = endpointId(member, serviceId);

    List<AbstractLongPollTask> waitForMe = new ArrayList<>();

    final MethodRegistry methodRegistry = new MethodRegistry(newLenientObjectMapper());
    methodRegistry.scan(new WorkerServiceImpl(e -> {
      e.printStackTrace();
      System.exit(1);
    }));

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
        leader = new LeaderTask(consul, Sandbox.serviceName, documentKeys).start();
      }

      @Override
      public void stopLeading() {
        checkState(leader != null, "Wasn't leading");
        // todo interrupt the leader thread.
        leader.stop();
        leader = null;
      }
    };

    try (SessionTask session =
             new SessionTask(consul, serviceName, serviceId, errorConsumer).start();

         RpcServerTask rpc =
             new RpcServerTask(dispatcher, consul.keyValueClient(), documentKeys, session.sessionId(), endpointId, errorConsumer).start();

         LeaderElectionTask election =
             new LeaderElectionTask(consul.keyValueClient(), documentKeys, session.sessionId(), errorConsumer, leaderController).start()) {

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

          Consul.newClient().agentClient().fail(serviceId, "(" + serviceId + ") Connector process terminated.");

        } catch (Exception e) {
          System.err.println("Failed to report termination to Consul agent.");
          e.printStackTrace();
        }
      });
      Runtime.getRuntime().addShutdownHook(shutdownHook);

      fatalErrorQueue.take();

    } finally {
      if (shutdownHook != null) {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
      }

      // Cancel any outstanding I/O operations such as the long polls for leader election;
      // see https://github.com/rickfast/consul-client/issues/307
      consul.destroy();

      for (AbstractLongPollTask task : waitForMe) {
        task.awaitTermination();
      }
    }

    System.out.println("Done");
  }
}
