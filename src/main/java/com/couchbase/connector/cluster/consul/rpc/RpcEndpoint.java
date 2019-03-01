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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.therapi.jsonrpc.client.ServiceFactory;
import com.orbitz.consul.KeyValueClient;

import java.time.Duration;

import static com.github.therapi.jackson.ObjectMappers.newLenientObjectMapper;
import static java.util.Objects.requireNonNull;

/**
 * Used by RPC client to create service instances that talk to remote server.
 */
public class RpcEndpoint {
  private static final ObjectMapper rpcObjectMapper = newLenientObjectMapper();

  private final ConsulRpcTransport transport;
  private final ServiceFactory serviceFactory;

  public RpcEndpoint(KeyValueClient kv, ConsulDocumentWatcher watcher, String endpointKey, Duration timeout) {
    this(new ConsulRpcTransport(kv, watcher, endpointKey, timeout));
  }

  public RpcEndpoint(ConsulRpcTransport transport) {
    this.transport = requireNonNull(transport);
    this.serviceFactory = new ServiceFactory(rpcObjectMapper, transport);
  }

  public <T> T service(Class<T> remotableServiceInterface) {
    return serviceFactory.createService(remotableServiceInterface);
  }

  public RpcEndpoint withTimeout(Duration timeout) {
    return new RpcEndpoint(transport.withTimeout(timeout));
  }

  @Override
  public String toString() {
    return transport.toString();
  }
}
