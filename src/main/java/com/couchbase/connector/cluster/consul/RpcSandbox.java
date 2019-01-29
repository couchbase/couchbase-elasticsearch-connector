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

import com.couchbase.connector.cluster.consul.rpc.RpcEndpoint;
import com.orbitz.consul.Consul;

import java.time.Duration;

import static com.couchbase.connector.cluster.consul.rpc.RpcHelper.listRpcEndpoints;


public class RpcSandbox {

  public static void main(String[] args) {
    final Consul consul = Consul.newClient();

    //final List<ServiceHealth> healthyServices = consul.healthClient().getHealthyServiceInstances(Sandbox.serviceName).getResponse();

    final Duration defaultTimeout = Duration.ofSeconds(30);

//    for (ServiceHealth serviceHealth : healthyServices) {
//      final String endpointId = String.join("::",
//          serviceHealth.getNode().getNode(),
//          serviceHealth.getNode().getAddress(),
//          serviceHealth.getService().getId());
//      final String endpointKey = rpcEndpointKey(Sandbox.serviceName, endpointId);

    for (RpcEndpoint endpoint : listRpcEndpoints(consul.keyValueClient(), Sandbox.serviceName, defaultTimeout)) {
      System.out.println(endpoint);

      final WorkerService follower = endpoint.service(WorkerService.class);

      System.out.println(follower.metrics());
      follower.ping();
      follower.sleep(3);

//
//      System.out.println("Before sleep 3");
//      follower.sleep(3);
//      System.out.println("After sleep 3");
//
//      System.out.println("Before sleep 16");
//
//      try {
//        endpoint.withTimeout(Duration.ofSeconds(1))
//            .service(FollowerService.class)
//            .sleep(16);
//      } catch (Exception e) {
//        e.printStackTrace();
//      }
////
//      System.out.println("After sleep 16");
    }

    System.out.println("done");

//    final Member member = consul.agentClient().getAgent().getMember();
//    final String endpointId = String.join("::", member.getName(), member.getAddress(), serviceId);
//
//
//    JsonRpcHttpClient client = new ConsulRpcClient(consul.keyValueClient(), )(objectMapper, jsonRpcRequest) -> {
//      System.out.println("Here in executor, request = " + jsonRpcRequest);
//      return mapper.createObjectNode().put("result", "Hello world");
//    };
//    ServiceFactory factory = new ServiceFactory(mapper, client);
//    GreetingService service = factory.createService(GreetingService.class);
//
//    System.out.println(service.greet("Harold"));

  }


}
