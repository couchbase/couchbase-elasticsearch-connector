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

import okhttp3.Headers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class ConsulResponse<T> {
  private final int httpStatusCode;
  private final String httpStatusMessage;

  private final String defaultAclPolicy;
  private final OptionalLong index;
  private final boolean knownLeader;
  private final Optional<Duration> lastContact;
  private final String queryBackend;

  private final Map<String, List<String>> headers;
  private final T body;

  private ConsulResponse(int httpStatusCode, String httpStatusMessage, String defaultAclPolicy, OptionalLong index, boolean knownLeader, Optional<Duration> lastContact, String queryBackend, Map<String, List<String>> headers, T body) {
    this.httpStatusCode = httpStatusCode;
    this.httpStatusMessage = httpStatusMessage;
    this.defaultAclPolicy = defaultAclPolicy;
    this.index = index;
    this.knownLeader = knownLeader;
    this.lastContact = lastContact;
    this.queryBackend = queryBackend;
    this.headers = headers;
    this.body = body;
  }

  public ConsulResponse(okhttp3.Response response, Function<okhttp3.Response, T> bodyExtractor) {
    Headers headers = response.headers();
    this.defaultAclPolicy = headers.get("X-Consul-Default-Acl-Policy");
    //this.index = Long.parseLong(headers.get(""));
    this.lastContact = Optional.ofNullable(headers.get("X-Consul-Lastcontact"))
        .map(Long::parseLong)
        .map(Duration::ofMillis);
    this.headers = headers.toMultimap();
    this.knownLeader = Optional.ofNullable(headers.get("X-Consul-Knownleader"))
        .map(Boolean::parseBoolean)
        .orElse(false);
    this.queryBackend = headers.get("X-Consul-Query-Backend");
    this.index = Optional.ofNullable(headers.get("X-Consul-Index"))
        .map(Long::parseUnsignedLong)
        .map(OptionalLong::of)
        .orElse(OptionalLong.empty());
    this.httpStatusCode = response.code();
    this.httpStatusMessage = response.message();
    this.body = bodyExtractor.apply(response);
  }

  public <R> ConsulResponse<R> withBody(R newBody) {
    return new ConsulResponse<>(
        this.httpStatusCode,
        this.httpStatusMessage,
        this.defaultAclPolicy,
        this.index,
        this.knownLeader,
        this.lastContact,
        this.queryBackend,
        this.headers,
        newBody);
  }

  public <R> ConsulResponse<R> map(Function<T,R> bodyTransform) {
    return withBody(bodyTransform.apply(this.body));
  }

  public String defaultAclPolicy() {
    return defaultAclPolicy;
  }

  public OptionalLong index() {
    return index;
  }

  public boolean knownLeader() {
    return knownLeader;
  }

  public Optional<Duration> lastContact() {
    return lastContact;
  }

  public String queryBackend() {
    return queryBackend;
  }

  public Map<String, List<String>> headers() {
    return headers;
  }

  public boolean isSuccessful() {
    return (httpStatusCode >= 200 &&  httpStatusCode <= 299);
  }

  public ConsulResponse<T> requireSuccess() {
    if (!isSuccessful()) {
      throw new RuntimeException(
          "Consul response had unexpected HTTP status code: " + httpStatusCode + " ; " + this);
    }
    return this;
  }

  public T body() {
    return body;
  }

  public int httpStatusCode() {
    return httpStatusCode;
  }

  public String httpStatusMessage() {
    return httpStatusMessage;
  }

  @Override
  public String toString() {
    return "ConsulResponse{" +
        "httpStatusCode=" + httpStatusCode +
        ", httpStatusMessage='" + httpStatusMessage + '\'' +
        ", defaultAclPolicy='" + defaultAclPolicy + '\'' +
        ", index=" + index +
        ", knownLeader=" + knownLeader +
        ", lastContact=" + lastContact +
        ", queryBackend='" + queryBackend + '\'' +
        ", headers=" + headers +
        ", body=" + body +
        '}';
  }
}
