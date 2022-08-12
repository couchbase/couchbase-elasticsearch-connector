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

package com.couchbase.consul.internal;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.function.Function;

public class OkHttpHelper {
  private OkHttpHelper() {
    throw new AssertionError("not instantiable");
  }

  public static <T> Mono<T> toMono(OkHttpClient client, Request request, Function<Response, T> responseTransformer) {
    return Mono.create(sink -> {
      Call call = client.newCall(request);

      sink.onCancel(call::cancel);

      call.enqueue(new Callback() {
        @Override
        public void onFailure(@NotNull Call call, @NotNull IOException e) {
          sink.error(e);
        }

        @Override
        public void onResponse(@NotNull Call call, @NotNull okhttp3.Response response) throws IOException {
          try (response) {
            sink.success(responseTransformer.apply(response));
          } catch (Throwable t) {
            sink.error(t);
          }
        }
      });
    });
  }

  public static Request withQueryParam(Request original, String paramName, String paramValue) {
    return original.newBuilder()
        .url(
            original.url().newBuilder()
                .setQueryParameter(paramName, paramValue)
                .build()
        )
        .build();
  }
}
