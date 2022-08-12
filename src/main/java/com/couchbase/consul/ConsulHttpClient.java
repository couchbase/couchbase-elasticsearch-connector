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

import com.couchbase.connector.config.common.ConsulConfig;
import com.couchbase.consul.internal.ConsulJacksonHelper;
import com.couchbase.consul.internal.OkHttpHelper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.net.HostAndPort;
import okhttp3.Dispatcher;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okio.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.couchbase.client.core.endpoint.http.CoreHttpPath.formatPath;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.removeStart;

public class ConsulHttpClient implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(ConsulHttpClient.class);

  private static class ConsulInternalServiceError extends RuntimeException {
    public ConsulInternalServiceError(String message) {
      super(message);
    }
  }

  private final JsonMapper mapper;

  private final Dispatcher dispatcher = new Dispatcher();

  {
    // The default limit of 5 concurrent requests per host is too restrictive for our use case.
    // Increase it to allow an effectively unlimited number of concurrent long-polling requests.
    dispatcher.setMaxRequests(Integer.MAX_VALUE);
    dispatcher.setMaxRequestsPerHost(Integer.MAX_VALUE);
  }

  private final OkHttpClient client = new OkHttpClient.Builder()
      .dispatcher(dispatcher)
      .callTimeout(ReadTimeoutSetter.CALL_TIMEOUT)
      .addInterceptor(new ReadTimeoutSetter())
      .build();

  private final HttpUrl baseUrl;
  private final ConsulConfig config;

  public ConsulHttpClient(HostAndPort address, ConsulConfig config) {
    this(address, config, ConsulJacksonHelper.consulJsonMapper());
  }

  public ConsulHttpClient(HostAndPort address, ConsulConfig config, JsonMapper mapper) {
    this.baseUrl = HttpUrl.parse("http://" + address.withDefaultPort(8500) + "/v1/");
    this.mapper = requireNonNull(mapper);
    this.config = requireNonNull(config);
  }

  public RequestBuilder get(String pathTemplate, String... pathArgs) {
    return new RequestBuilder(Method.GET, pathTemplate, pathArgs);
  }

  public RequestBuilder put(String pathTemplate, String... pathArgs) {
    return new RequestBuilder(Method.PUT, pathTemplate, pathArgs);
  }

  public RequestBuilder delete(String pathTemplate, String... pathArgs) {
    return new RequestBuilder(Method.DELETE, pathTemplate, pathArgs);
  }

  private HttpUrl baseUrl() {
    return baseUrl;
  }

  private OkHttpClient client() {
    return client;
  }

  private JsonMapper mapper() {
    return mapper;
  }

  @Override
  public void close() {
    client.dispatcher().executorService().shutdown();
    client.dispatcher().cancelAll();
    client.connectionPool().evictAll();
  }

  private static final MediaType JSON = MediaType.parse("application/json");
  private static final MediaType PLAINTEXT = MediaType.parse("text/plain;charset=UTF-8");

  enum Method {
    GET,
    PUT,
    DELETE,
  }

  public class RequestBuilder {
    private final String path;
    private final Map<String, Object> queryParams = new LinkedHashMap<>();
    private final Method method;
    private ByteString content;
    private MediaType contentType;

    public RequestBuilder(Method method, String pathTemplate, String... pathArgs) {
      this.method = method;
      this.path = removeStart(formatPath(pathTemplate, pathArgs), "/");
      if (this.method == Method.PUT) {
        this.content = ByteString.EMPTY; // otherwise OkHttp complains PUT must have a body
      }
    }

    public RequestBuilder query(Map<String, ?> params) {
      queryParams.putAll(params);
      return this;
    }

    public RequestBuilder query(String... valuelessParamName) {
      Arrays.stream(valuelessParamName).forEach(it -> query(it, ""));
      return this;
    }

    public RequestBuilder query(String name, Object value) {
      queryParams.put(name, value);
      return this;
    }

    public RequestBuilder blocking(long index, Duration wait) {
      return query(Map.of(
          "index", index,
          "wait", Math.min(1, wait.toSeconds()) + "s"));
    }

    public RequestBuilder bodyJson(Object body) {
      try {
        return setBody(mapper().writeValueAsBytes(body), JSON);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    public RequestBuilder bodyText(String text) {
      return setBody(text.getBytes(UTF_8), PLAINTEXT);
    }

    private RequestBuilder setBody(byte[] content, MediaType contentType) {
      this.content = ByteString.of(content);
      this.contentType = contentType;
      return this;
    }

    public <T> Mono<ConsulResponse<T>> buildWithResponseType(Class<T> responseBodyType) {
      return buildWithResponseType(new TypeReference<T>() {
        public Type getType() {
          return responseBodyType;
        }
      });
    }

    public <T> Mono<ConsulResponse<T>> buildWithResponseType(TypeReference<T> responseBodyType) {
      return OkHttpHelper.toMono(
          client(),
          toOkHttpRequest(),
          response -> new ConsulResponse<>(response, r -> {
            try {
              byte[] body = requireNonNull(response.body(), "expected non-null response body").bytes();

              // Throw an exception for certain fatal server errors, so we can retry.
              if (response.code() == 500 || response.code() == 503) {
                String msg = "Consul request [" + method + " " + path + "] failed with status code " + response.code() + ".";
                if (body.length > 0) {
                  msg += " Response body: " + new String(body, UTF_8);
                }
                throw new ConsulInternalServiceError(msg);
              }

              return body.length == 0 || responseBodyType == null || !response.isSuccessful() ? null : mapper().readValue(body, responseBodyType);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          })
      ).retryWhen(Retry
          .fixedDelay(10, Duration.ofSeconds(1))
          .jitter(0.25)
          .filter(it -> it instanceof ConsulInternalServiceError)
          .doBeforeRetry(it -> log.info("Retrying failed Consul request. " + it.failure().getMessage()))
      );
    }

    private Request toOkHttpRequest() {
      HttpUrl.Builder urlBuilder = baseUrl().newBuilder()
          .addEncodedPathSegments(path);
      queryParams.forEach((k, v) -> urlBuilder.addQueryParameter(k, (String.valueOf(v))));

      RequestBody body = content == null ? null : RequestBody.create(content, contentType);

      Request.Builder builder = new Request.Builder()
          .url(urlBuilder.build())
          .method(method.name(), body);
      config.aclToken().ifPresent(it -> builder.header("X-Consul-Token", it));
      return builder.build();
    }
  }
}
