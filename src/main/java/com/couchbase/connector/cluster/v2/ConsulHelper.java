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

package com.couchbase.connector.cluster.v2;

import com.orbitz.consul.ConsulException;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.kv.ImmutableOperation;
import com.orbitz.consul.model.kv.Value;
import com.orbitz.consul.model.kv.Verb;
import com.orbitz.consul.option.ImmutableQueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;

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


  public static String awaitChange(KeyValueClient kv, String key, String expectedValue) {
    requireNonNull(expectedValue);

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
        if (!expectedValue.equals(valueAsString)) {
          LOGGER.debug("New value for key {}: {}", key, valueAsString);
          return valueAsString;
        }

        index = response.getIndex();
      }
    }
  }
}
