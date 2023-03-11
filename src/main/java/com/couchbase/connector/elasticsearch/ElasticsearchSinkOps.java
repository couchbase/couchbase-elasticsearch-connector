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

package com.couchbase.connector.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.MgetResponse;
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.couchbase.connector.elasticsearch.io.BackoffPolicy;
import com.couchbase.connector.elasticsearch.sink.Operation;
import com.couchbase.connector.elasticsearch.sink.SinkBulkResponse;
import com.couchbase.connector.elasticsearch.sink.SinkBulkResponseItem;
import com.couchbase.connector.elasticsearch.sink.SinkErrorCause;
import com.couchbase.connector.elasticsearch.sink.SinkOps;
import com.couchbase.connector.elasticsearch.sink.SinkTestOps;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static com.couchbase.client.core.endpoint.http.CoreHttpPath.formatPath;
import static com.couchbase.client.core.util.CbCollections.transform;
import static com.couchbase.connector.elasticsearch.io.BackoffPolicyBuilder.constantBackoff;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings("DuplicatedCode") // Same code as OpenSearchSinkOps, but uses classes from different packages.
public class ElasticsearchSinkOps implements SinkOps, SinkTestOps {
  private static final Logger log = LoggerFactory.getLogger(ElasticsearchSinkOps.class);

  private static final JsonMapper jsonMapper = new JsonMapper();

  private final ElasticsearchClient client;
  private final RestClient lowLevelClient;

  private final String bulkRequestTimeoutString;

  public ElasticsearchSinkOps(RestClientBuilder restClientBuilder, Duration bulkRequestTimeout) {
    this.lowLevelClient = restClientBuilder.build();
    this.bulkRequestTimeoutString = bulkRequestTimeout.toMillis() + "ms";

    ElasticsearchTransport transport = new RestClientTransport(
        lowLevelClient,
        new JacksonJsonpMapper(jsonMapper)
    );

    this.client = new ElasticsearchClient(transport);
  }

  @Override
  public SinkBulkResponse bulk(List<Operation> operations) throws IOException {
    ElasticsearchBulkRequestBuilder requestBuilder = new ElasticsearchBulkRequestBuilder();
    operations.forEach(it -> it.addTo(requestBuilder));
    BulkRequest request = requestBuilder.build(bulkRequestTimeoutString);

    BulkResponse wrapped = client.bulk(request);
    return new SinkBulkResponse() {
      @Override
      public Duration ingestTook() {
        return wrapped.ingestTook() == null ? Duration.ZERO : Duration.ofNanos(wrapped.ingestTook());
      }

      @Override
      public List<SinkBulkResponseItem> items() {
        return transform(wrapped.items(), it -> new SinkBulkResponseItem() {
          @Override
          public int status() {
            return it.status();
          }

          @Override
          public SinkErrorCause error() {
            return it.error() == null ? null : new SinkErrorCause() {
              @Nullable
              @Override
              public String reason() {
                return it.error().reason();
              }

              @Override
              public String toString() {
                return it.error().toString();
              }
            };
          }
        });
      }
    };
  }

  @Override
  public ObjectNode info() {
    // Can't do it the easy way due to https://github.com/elastic/elasticsearch-java/issues/346
    // So use low level client instead :-p
    try {
      final Response response = lowLevelClient.performRequest(new Request("GET", "/"));
      try (InputStream is = response.getEntity().getContent()) {
        return (ObjectNode) jsonMapper.readTree(is);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<JsonNode> getDocument(String index, String id, @Nullable String routing) {
    try {
      String url = formatPath("{}/_doc/{}", index, id);
      if (routing != null) {
        url += formatPath("?routing={}", routing);
      }
      return Optional.of(doGet(url));

    } catch (ResponseException e) {
      return Optional.empty();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long countDocuments(String index) {
    try {
      JsonNode response = doGet(index + "/_count");
      return response.get("count").longValue();
    } catch (ResponseException e) {
      return -1;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void deleteAllIndexes() {
    try {
      // retry for intermittent auth failure immediately after container startup :-p
      final BackoffPolicy backoffPolicy = constantBackoff(Duration.ofSeconds(1)).limit(10).build();

      for (String index : retryUntilSuccess(backoffPolicy, this::indexNames)) {
        if (index.startsWith(".")) {
          // ES 5.x complains if we try to delete these.
          continue;
        }
        JsonNode deletionResponse = doDelete(index);
        System.out.println("Deleted index '" + index + "' : " + deletionResponse);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<MultiGetItem> multiGet(String index, Collection<String> ids) {
    List<MultiGetItem> result = new ArrayList<>();
    try {
      final MgetResponse<JsonNode> response = client.mget(
          builder -> builder
              .index(index)
              .ids(List.copyOf(Set.copyOf(ids))),
          JsonNode.class
      );

      for (MultiGetResponseItem<JsonNode> item : response.docs()) {
        if (item.isFailure()) {
          result.add(new MultiGetItem(item.failure().id(), item.failure().error().reason(), null));
        } else {
          result.add(new MultiGetItem(item.result().id(), null, item.result().source()));
        }
      }
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private JsonNode doDelete(String endpoint) throws IOException {
    final Response response = lowLevelClient.performRequest(new Request("DELETE", endpoint));
    try (InputStream is = response.getEntity().getContent()) {
      return jsonMapper.readTree(is);
    }
  }

  private static <T> T retryUntilSuccess(BackoffPolicy backoffPolicy, Callable<T> lambda) {
    Iterator<Duration> delays = backoffPolicy.iterator();
    while (true) {
      try {
        return lambda.call();
      } catch (Exception e) {
        e.printStackTrace();

        if (delays.hasNext()) {
          try {
            MILLISECONDS.sleep(delays.next().toMillis());
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(interrupted);
          }
        } else {
          throw new RuntimeException(new TimeoutException());
        }
      }
    }
  }

  private List<String> indexNames() {
    try {
      final JsonNode response = doGet("_stats");
      return ImmutableList.copyOf(response.path("indices").fieldNames());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private JsonNode doGet(String endpoint) throws IOException {
    final Response response = lowLevelClient.performRequest(new Request("GET", endpoint));
    try (InputStream is = response.getEntity().getContent()) {
      return jsonMapper.readTree(is);
    }
  }


  @Override
  public void close() throws IOException {
    client._transport().close();
  }
}
