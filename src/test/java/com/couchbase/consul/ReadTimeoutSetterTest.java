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

import com.couchbase.consul.internal.OkHttpHelper;
import okhttp3.Request;
import org.junit.Test;

import java.time.Duration;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class ReadTimeoutSetterTest {

  @Test
  public void getWait() throws Exception {
    Request r = new Request.Builder()
        .url("http://127.0.0.1")
        .build();
    assertEquals(
        Optional.empty(),
        ReadTimeoutSetter.getWait(r)
    );

    r = OkHttpHelper.withQueryParam(r, "wait", "");
    assertEquals(
        Optional.empty(),
        ReadTimeoutSetter.getWait(r)
    );

    r = OkHttpHelper.withQueryParam(r, "wait", "7m");
    assertEquals(
        Optional.of(Duration.ofMinutes(7)),
        ReadTimeoutSetter.getWait(r)
    );

    r = OkHttpHelper.withQueryParam(r, "wait", "20s");
    assertEquals(
        Optional.of(Duration.ofSeconds(20)),
        ReadTimeoutSetter.getWait(r)
    );
  }

  @Test
  public void accountsForConsulJitter() throws Exception {
    Duration d = Duration.ofSeconds(100);
    assertEquals(
        Duration.ofSeconds(117), // original, + 6.25% (rounded up to 7) + 10 seconds grace period
        ReadTimeoutSetter.addConsulJitter(d)
    );
  }
}
