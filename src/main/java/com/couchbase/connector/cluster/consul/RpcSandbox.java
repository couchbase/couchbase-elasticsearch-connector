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
import com.github.therapi.core.MethodRegistry;
import com.github.therapi.core.annotation.Default;
import com.github.therapi.core.annotation.Remotable;
import com.github.therapi.jsonrpc.client.JsonRpcHttpClient;
import com.github.therapi.jsonrpc.client.ServiceFactory;
import com.orbitz.consul.Consul;
import com.orbitz.consul.model.health.ServiceHealth;

import java.util.List;

import static com.github.therapi.jackson.ObjectMappers.newLenientObjectMapper;

public class RpcSandbox {


  @Remotable("greeting")
  public interface GreetingService {

    default String greet(@Default("stranger") String name) {
      System.out.println("Greeting " + name + "!");
      return "Hello, " + name + "!";
    }
  }

  public static void main(String[] args) {
    ObjectMapper mapper = newLenientObjectMapper();//new ObjectMapper();
    MethodRegistry methodRegistry = new MethodRegistry(mapper);
    methodRegistry.scan(new GreetingService(){});

    //JsonNode result = methodRegistry.invoke("greeting.greet", mapper.createArrayNode());
    //System.out.println(result);

//    JsonRpcDispatcher jsonRpcDispatcher = JsonRpcDispatcher.builder(methodRegistry)
//        .build();
//
//    Optional<JsonNode> result = jsonRpcDispatcher.invoke("{id:1,method:'greeting.greet', params:['Harold']}");
//    System.out.println(result.orElse(null));

    final Consul consul = Consul.newClient();

    final List<ServiceHealth> healthyServices = consul.healthClient().getHealthyServiceInstances(Sandbox.serviceName).getResponse();

    for (ServiceHealth serviceHealth : healthyServices) {
      final String endpointId = String.join("::",
          serviceHealth.getNode().getNode(),
          serviceHealth.getNode().getAddress(),
          serviceHealth.getService().getId());

      JsonRpcHttpClient client = new ConsulRpcTransport(consul.keyValueClient(), Sandbox.serviceName, endpointId);
      ServiceFactory factory = new ServiceFactory(mapper, client);
      FollowerService service = factory.createService(FollowerService.class);


      System.out.println(endpointId + " -> " + service.metrics());


      service.ping();
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
