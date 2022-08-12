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

import com.couchbase.client.core.util.Golang;
import com.couchbase.consul.internal.OkHttpHelper;
import com.google.common.annotations.VisibleForTesting;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.util.CbStrings.emptyToNull;

/**
 * Sets the read timeout to match the "wait" query parameter of
 * Consul blocking requests.
 */
public class ReadTimeoutSetter implements Interceptor {
  private static final Duration MAX_WAIT = Duration.ofMinutes(10);

  /**
   * Maximum amount of time any Consul request is expected to take.
   * <p>
   * (There's no easy way to set the call timeout on a per-request basis, unfortunately;
   * see <a href="https://github.com/square/retrofit/issues/3434">Retrofit issue 3434</a>)
   */
  public static final Duration CALL_TIMEOUT = addConsulJitter(MAX_WAIT)
      .plus(Duration.ofSeconds(10)); // *extra* grace period for response processing, etc.

  @NotNull
  @Override
  public Response intercept(@NotNull Chain chain) throws IOException {
    Request request = chain.request();
    boolean hasIndex = request.url().queryParameterNames().contains("index");

    if (!hasIndex) {
      // It's not a blocking request, so honor the client's configured timeout.
      return chain.proceed(request);
    }

    Duration wait = getWait(request).orElse(null);
    if (wait == null) {
      // Make the default wait time explicit, in case it changes in a future Consul version.
      request = OkHttpHelper.withQueryParam(request, "wait", "5m");
      wait = getWait(request).orElseThrow();
    }

    if (wait.compareTo(MAX_WAIT) > 0) {
      throw new IllegalArgumentException("Specified wait " + wait + " is greater than max wait " + MAX_WAIT);
    }

    wait = addConsulJitter(wait);

    return chain
        .withReadTimeout((int) wait.toMillis(), TimeUnit.MILLISECONDS)
        .proceed(request);
  }

  /**
   * Returns a Duration slightly longer than the given duration.
   * Accounts for the 6.25% (1/16) jitter added by Consul, plus some grace time.
   * <p>
   * See <a href="https://www.consul.io/api-docs/features/blocking">Consul Blocking Queries.</a>
   */
  @VisibleForTesting
  static Duration addConsulJitter(Duration wait) {
    int gracePeriodSeconds = 10;
    int waitSecondsPlusJitter = Math.toIntExact((long) Math.ceil(wait.toSeconds() * 1.0625));
    return Duration.ofSeconds(gracePeriodSeconds + waitSecondsPlusJitter);
  }

  @VisibleForTesting
  static Optional<Duration> getWait(Request r) {
    String wait = r.url().queryParameter("wait");
    return Optional.ofNullable(emptyToNull(wait))
        .map(ReadTimeoutSetter::parseWait);
  }

  private static Duration parseWait(String s) {
    // Consul's duration format is a subset of Go's duration format.
    return Golang.parseDuration(s);
  }

}
