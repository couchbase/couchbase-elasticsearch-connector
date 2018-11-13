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

import com.orbitz.consul.Consul;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MINUTES;

public class Sandbox {
  private static final Logger LOGGER = LoggerFactory.getLogger(Sandbox.class);

  public static void main(String[] args) throws Exception {
    final String serviceName = "service-registration-test";
    //final String serviceId = "zero";
    final String serviceId = "one";
    final Consumer<Throwable> errorConsumer = e -> LOGGER.error("Got fatal error", e);

    final Consul consul = Consul.newClient();
    LeaderElectionTask waitForMe = null;
    Thread shutdownHook = null;

    try (SessionTask session =
             new SessionTask(consul, serviceName, serviceId, errorConsumer).start();

         LeaderElectionTask election =
             new LeaderElectionTask(consul.keyValueClient(), serviceName, session.sessionId(), errorConsumer).start()) {


      waitForMe = election;


      shutdownHook = new Thread(() -> {
        try {
          System.out.println("Shutting down");

          session.close(); // to prevent race with updating (failing) the health check
          election.close();
          consul.destroy();
          election.awaitTermination();

          Consul.newClient().agentClient().fail(serviceId, "(" + serviceId + ") Connector process terminated.");

        } catch (Exception e) {
          System.err.println("Failed to report termination to Consul agent.");
          e.printStackTrace();
        }
      });
      Runtime.getRuntime().addShutdownHook(shutdownHook);


      MINUTES.sleep(5);

    } finally {
      if (shutdownHook != null) {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
      }

      // Cancel any outstanding I/O operations such as the long polls for leader election;
      // see https://github.com/rickfast/consul-client/issues/307
      consul.destroy();

      if (waitForMe != null) {
        waitForMe.awaitTermination();
      }
    }

    System.out.println("Done");
  }
}
