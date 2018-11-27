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

import com.orbitz.consul.ConsulException;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.kv.ImmutableOperation;
import com.orbitz.consul.model.kv.Value;
import com.orbitz.consul.model.kv.Verb;
import com.orbitz.consul.option.ImmutablePutOptions;
import com.orbitz.consul.option.ImmutableQueryOptions;
import com.orbitz.consul.option.PutOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class ConsulHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsulHelper.class);

  private ConsulHelper() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Returns {@code true} if the given session owned the lock, otherwise {@code false}.
   */
  public static boolean unlockAndDelete(KeyValueClient kv, String key, String sessionId) {
    try {
      kv.performTransaction(
          ImmutableOperation.builder(Verb.UNLOCK).key(key).session(sessionId).build(),
          ImmutableOperation.builder(Verb.DELETE).key(key).build());
      return true;

    } catch (ConsulException e) {
      if (e.getCode() == HTTP_CONFLICT) {
        return false; // didn't own lock; no worries
      }
      throw e;
    }
  }

  public static void awaitRemoval(KeyValueClient kv, String key) {
    BigInteger index = BigInteger.ZERO;

    while (true) {
      final ConsulResponse<Value> response = kv.getConsulResponseWithValue(key,
          ImmutableQueryOptions.builder()
              .index(index)
              .wait("5m")
              .build())
          .orElse(null);
      if (response == null) {
        return;
      }

      index = response.getIndex();
    }
  }

  private static final String missingDocumentValue = "";

  private static void atomicUpdate(KeyValueClient kv, String key, Function<String, String> mutator) {
    while (true) {
      final ConsulResponse<Value> r = kv.getConsulResponseWithValue(key).orElse(null);
      final BigInteger index = r == null ? BigInteger.ZERO : r.getIndex();
      final String oldValue = r == null ? missingDocumentValue : r.getResponse().getValueAsString(UTF_8).orElse(missingDocumentValue);
      final String newValue = mutator.apply(oldValue);

      if (Objects.equals(newValue, oldValue)) {
        return;
      }

      final PutOptions options = ImmutablePutOptions.builder().cas(index.longValue()).build();
      boolean success = kv.putValue(key, newValue, 0, options, UTF_8);
      if (success) {
        return;
      }

      // todo truncated exponential backoff, please! Die if timeout!
      //MILLISECONDS.sleep(100);
    }
  }

  public static void atomicUpdate(KeyValueClient kv, ConsulResponse<Value> initialResponse, Function<String, String> mutator) {
    final Value v = initialResponse.getResponse();
    final String key = v.getKey();

    LOGGER.info("Updating key {}", key);

    final String oldValue = v.getValueAsString(UTF_8).orElse(missingDocumentValue);
    final String newValue = mutator.apply(oldValue);

    if (Objects.equals(newValue, oldValue)) {
      return;
    }

    final long index = initialResponse.getIndex().longValue();
    final PutOptions options = ImmutablePutOptions.builder().cas(index).build();
    boolean success = kv.putValue(key, newValue, 0, options, UTF_8);

    if (!success) {
      LOGGER.info("Failed to put new document (optimistic locking failure?); reloading and retrying");
      atomicUpdate(kv, key, mutator);
    }
  }

  public static ConsulResponse<Value> awaitChange(KeyValueClient kv, String key, BigInteger index) {
    while (true) {
      final ConsulResponse<Value> response = kv.getConsulResponseWithValue(key,
          ImmutableQueryOptions.builder()
              .index(index)
              .wait("5m")
              .build())
          .orElse(null);
      if (response == null) {
        LOGGER.debug("Document does not exist: {}", key);
        return null;
      }

      if (index.equals(response.getIndex())) {
        LOGGER.debug("Long poll timed out, polling again for {}", key);
      } else {
        return response;
      }
    }
  }

  public static String awaitCondition(KeyValueClient kv, String key, Predicate<String> condition) {
    return awaitCondition(kv, key, value -> value, condition);
  }

  public static <T> T awaitCondition(KeyValueClient kv, String key, Function<String, T> mapper, Predicate<T> condition) {
    requireNonNull(condition);

    BigInteger index = BigInteger.ZERO;

    while (true) {
      final ConsulResponse<Value> response = kv.getConsulResponseWithValue(key,
          ImmutableQueryOptions.builder()
              .index(index)
              .wait("5m")
              .build())
          .orElse(null);
      if (response == null) {
        LOGGER.debug("Document does not exist: {}", key);
        return null;
      }

      if (index.equals(response.getIndex())) {
        LOGGER.debug("Long poll timed out, polling again for {}", key);
      } else {
        final String valueAsString = response.getResponse().getValueAsString(UTF_8).orElse(null);
        final T mappedValue = mapper.apply(valueAsString);
        if (condition.test(mappedValue)) {
          LOGGER.debug("New value for key {}: {}", key, valueAsString);
          return mappedValue;
        }

        index = response.getIndex();
      }
    }
  }

  public static String rpcEndpointKey(String serviceName, String endpointId) {
    return "couchbase/cbes/" + requireNonNull(serviceName) + "/rpc/" + requireNonNull(endpointId);
  }
}
