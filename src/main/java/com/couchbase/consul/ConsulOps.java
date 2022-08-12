/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.consul;

import com.couchbase.connector.cluster.consul.ConsulDocumentWatcher;
import com.couchbase.connector.cluster.consul.ConsulResourceWatcher;
import com.couchbase.connector.config.common.ConsulConfig;
import com.couchbase.connector.config.common.ImmutableConsulConfig;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class ConsulOps implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(ConsulOps.class);

  private final TypeReference<List<String>> LIST_OF_STRINGS = new TypeReference<>() {
  };
  private final TypeReference<List<KvReadResult>> LIST_OF_KV_READ_RESULTS = new TypeReference<>() {
  };

  public static final ConsulConfig DEFAULT_CONFIG = ImmutableConsulConfig.builder().build();

  private static final Duration MAX_WAIT = Duration.ofMinutes(6);

  // Add 10% over MAX_WAIT to account for the wait/16 jitter added by Consul, plus some grace time.
  private final Duration TIMEOUT = Duration.ofMillis((long) (MAX_WAIT.toMillis() * 1.1));

  private final ConsulHttpClient httpClient;

  private final KvOps kvOps;
  private final AgentOps agentOps;
  private final SessionOps sessionOps;
  private final HealthOps healthOps;

  public class SessionOps {
    public Mono<ConsulResponse<String>> createSession(Map<String, ?> session, Map<String, ?> params) {
      return httpClient.put("session/create")
          .query(params)
          .bodyJson(session)
          .buildWithResponseType(ObjectNode.class)
          .map(ConsulResponse::requireSuccess)
          .map(response -> response.map(json -> requireNonNull(
              json.path("ID").textValue(),
              "session/create response missing ID property"))
          );
    }

  }

  public class HealthOps {
    public Mono<ConsulResponse<ArrayNode>> health(String service, Map<String, ?> params) {
      return httpClient.get("health/service/{}", service)
          .query(params)
          .buildWithResponseType(ArrayNode.class)
          .map(ConsulResponse::requireSuccess);
    }
  }

  public class AgentOps {
    public Mono<ConsulResponse<Void>> register(Map<String, ?> registration, Map<String, ?> params) {
      return httpClient.put("agent/service/register")
          .query(params)
          .bodyJson(registration)
          .buildWithResponseType(Void.class)
          .map(ConsulResponse::requireSuccess);
    }

    public Mono<ConsulResponse<Void>> deregister(String serviceId) {
      return httpClient.put("agent/service/deregister/{}", serviceId)
          .buildWithResponseType(Void.class);
    }

    public Mono<ConsulResponse<Void>> check(String state, String checkId, Map<String, ?> params) {
      return httpClient.put("agent/check/{}/{}", state, checkId)
          .query(params)
          .buildWithResponseType(Void.class)
          .map(ConsulResponse::requireSuccess);
    }

    public Mono<ConsulResponse<ObjectNode>> self() {
      return httpClient.get("agent/self")
          .buildWithResponseType(ObjectNode.class)
          .map(ConsulResponse::requireSuccess);
    }
  }

  public static class CancelMutationException extends RuntimeException {
  }

  public class KvOps {


    public void mutate(String key, Function<Optional<String>, Optional<String>> mutator) {

    }


    public ConsulDocumentWatcher documentWatcher() {
      return new ConsulDocumentWatcher(ConsulOps.this);
    }

    public Flux<Optional<String>> watchOneKey(String key) {
      return documentWatcher().watch(key);
    }

    public Flux<List<String>> watchKeys(String prefix) {
      return new ConsulResourceWatcher()
          .watch(opt -> listKeys("rpc/", opt))
          .map(ConsulResponse::body)
          .distinctUntilChanged()
          .doOnError(t -> log.warn("Watch on key prefix '" + prefix + "' terminated due to error.", t))
          .doFinally(signal -> log.info("Watch on key prefix '" + prefix + "' terminated: " + signal));
    }

    public Mono<Optional<String>> readOneKeyAsString(String key) {
      return readOneKey(key)
          .map(ConsulResponse::body)
          .map(it -> it.map(KvReadResult::valueAsString));
    }

    public Mono<ConsulResponse<Optional<KvReadResult>>> readOneKey(String key) {
      return readOneKey(key, Map.of());
    }

    public Mono<ConsulResponse<Optional<KvReadResult>>> readOneKey(String key, Map<String, ?> params) {
      return httpClient.get("kv/{}", key)
          .query(params)
          .buildWithResponseType(LIST_OF_KV_READ_RESULTS)
          .doOnSubscribe(it -> log.debug("KV read; key = {} ; params = {}", key, params))
          .doOnNext(it -> log.trace("KV read result: {}", it))
          .map(it -> it.httpStatusCode() == 404
              ? it.withBody(Optional.empty())
              : it.requireSuccess().withBody(Optional.of(it.body().get(0))));
    }

    public Mono<ConsulResponse<List<KvReadResult>>> readKeys(String key, Map<String, ?> params) {
      return httpClient.get("kv/{}", key)
          .query(params)
          .buildWithResponseType(LIST_OF_KV_READ_RESULTS)
          .map(it -> it.httpStatusCode() == 404 ? it.withBody(List.of()) : it.requireSuccess());
    }

    public Mono<ConsulResponse<List<String>>> listKeys(String prefix, Map<String, Object> params) {
      return httpClient.get("kv/{}", prefix)
          .query(params)
          .query("keys", "")
          .buildWithResponseType(LIST_OF_STRINGS)
          .map(it -> it.httpStatusCode() == 404 ? it.withBody(List.of()) : it.requireSuccess());
    }

    public Mono<ConsulResponse<Boolean>> upsertKey(String key, String value) {
      return upsertKey(key, value, Map.of());
    }

    public Mono<ConsulResponse<Boolean>> upsertKey(String key, String value, Map<String, ?> params) {
      return httpClient.put("kv/{}", key)
          .query(params)
          .bodyText(value)
          .buildWithResponseType(Boolean.class)
          .doOnSubscribe(it -> log.trace("KV upsert; key = {} ; params = {} value = {}", key, params, value))
          .doOnNext(it -> log.trace("KV upsert result: {}", it))
          .map(ConsulResponse::requireSuccess);
    }

    public Mono<ConsulResponse<Boolean>> deleteKey(String key) {
      return httpClient.delete("kv/{}", key)
          .buildWithResponseType(Boolean.class)
          .map(ConsulResponse::requireSuccess);
    }
  }

  public ConsulOps(HostAndPort address, ConsulConfig config) {
    this.httpClient = new ConsulHttpClient(address, config);
    this.kvOps = new KvOps();
    this.agentOps = new AgentOps();
    this.sessionOps = new SessionOps();
    this.healthOps = new HealthOps();
  }

  public Duration getMaxWait() {
    return MAX_WAIT;
  }

  public ConsulHttpClient httpClient() {
    return httpClient;
  }

  public KvOps kv() {
    return kvOps;
  }

  public AgentOps agent() {
    return agentOps;
  }

  public SessionOps session() {
    return sessionOps;
  }

  public HealthOps health() {
    return healthOps;
  }

  @Override
  public void close() {
    httpClient.close();
  }

}
