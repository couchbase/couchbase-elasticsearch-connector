/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.connector.elasticsearch;

import com.couchbase.client.core.util.Deadline;
import com.couchbase.client.dcp.util.Version;
import com.couchbase.connector.util.ThrowableHelper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ConnectionClosedException;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.connector.elasticsearch.io.MoreBackoffPolicies.truncatedExponentialBackoff;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ElasticsearchVersionSniffer {
  private static final Logger log = LoggerFactory.getLogger(ElasticsearchVersionSniffer.class);

  private final CloseableHttpAsyncClient httpClient;

  public ElasticsearchVersionSniffer(CloseableHttpAsyncClient httpClient) {
    this.httpClient = requireNonNull(httpClient);
  }

  public FlavorAndVersion sniff(List<HttpHost> hosts, Duration timeout) {
    Deadline deadline = Deadline.of(timeout);

    final Iterator<Duration> retryDelays = truncatedExponentialBackoff(
        Duration.ofSeconds(1), Duration.ofMinutes(1)).iterator();

    while (true) {
      List<Throwable> failures = new CopyOnWriteArrayList<>();

      FlavorAndVersion result = Flux.fromIterable(hosts)
          .flatMap(host -> this.sniff(host)
              .onErrorResume(t -> {
                failures.add(t);
                return Mono.empty();
              })
          )
          .blockFirst(timeout);

      if (result != null) {
        check(result);
        return result;
      }

      if (deadline.exceeded()) {
        throw new RuntimeException("Failed to connect to sink within " + timeout);
      }

      final Duration delay = retryDelays.next();

      for (Throwable t : failures) {
        log.warn("Failed to connect to sink. Retrying in {}", delay, t);
        if (ThrowableHelper.hasCause(t, ConnectionClosedException.class)) {
          log.warn("  Troubleshooting tip: If the sink connection failure persists," +
              " and the sink is configured to require TLS, then make sure the connector is also configured to use secure connections.");
        }

        try {
          MILLISECONDS.sleep(delay.toMillis());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
    }
  }

  private void check(FlavorAndVersion result) {
    log.info("Detected sink: {}", result);

    final Version version = result.version;
    final Version required = result.flavor.minCompatible;
    final Version recommended = result.flavor.minRecommended;
    if (version.compareTo(required) < 0) {
      throw new RuntimeException(result.flavor + " version " + required + " or later is required; actual version is " + version);
    }
    if (version.compareTo(recommended) < 0) {
      log.warn(result.flavor + " version " + version + " is lower than recommended minimum version " + recommended + ".");
    }
  }

  private Mono<SimpleHttpResponse> execReactive(Method method, HttpHost host, String path) {
    return Mono.create(sink -> {
      AtomicReference<Future<SimpleHttpResponse>> future = new AtomicReference<>();

      sink.onCancel(() -> {
        Future<SimpleHttpResponse> f = future.get();
        if (f != null) {
          f.cancel(true);
        }
      });

      SimpleHttpRequest request = SimpleHttpRequest.create(method, host, path);
      future.set(httpClient.execute(request, new FutureCallback<>() {
        @Override
        public void completed(SimpleHttpResponse result) {
          sink.success(result);
        }

        @Override
        public void failed(Exception ex) {
          sink.error(ex);
        }

        @Override
        public void cancelled() {
          sink.error(new CancellationException("The HTTP request was cancelled."));
        }
      }));
    });
  }

  private static final JsonMapper jsonMapper = JsonMapper.builder().build();

  private Mono<FlavorAndVersion> sniff(HttpHost host) {
    log.info("Getting sink flavor and version from {}", redactSystem(host));
    return execReactive(Method.GET, host, "/")
        .map(response -> {
          try {
            JsonNode info = jsonMapper.readTree(response.getBodyBytes());
            return new FlavorAndVersion(
                Flavor.of(info.at("/version/distribution").textValue()),
                Version.parseVersion(info.at("/version/number").textValue())
            );
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  public enum Flavor {
    ELASTICSEARCH("Elasticsearch", new Version(7, 14, 0), new Version(7, 17, 5)),
    OPENSEARCH("OpenSearch", new Version(1, 3, 3), new Version(2, 6, 0)),
    ;

    private final String prettyName;
    private final Version minCompatible;
    private final Version minRecommended;

    Flavor(String prettyName, Version minCompatible, Version minRecommended) {
      this.minCompatible = requireNonNull(minCompatible);
      this.minRecommended = requireNonNull(minRecommended);
      this.prettyName = requireNonNull(prettyName);
    }

    @Override
    public String toString() {
      return prettyName;
    }

    public Version minCompatible() {
      return minCompatible;
    }

    public Version minRecommended() {
      return minRecommended;
    }

    public static Flavor of(@Nullable String s) {
      return "opensearch".equals(s)
          ? OPENSEARCH
          : ELASTICSEARCH;
    }
  }

  public static class FlavorAndVersion {
    public final Flavor flavor;
    public final Version version;

    public FlavorAndVersion(Flavor flavor, Version version) {
      this.flavor = requireNonNull(flavor);
      this.version = requireNonNull(version);
    }

    @Override
    public String toString() {
      return flavor + " " + version;
    }
  }
}
