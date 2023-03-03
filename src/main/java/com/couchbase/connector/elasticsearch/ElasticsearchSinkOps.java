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
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.couchbase.client.dcp.util.Version;
import com.couchbase.connector.elasticsearch.sink.Operation;
import com.couchbase.connector.elasticsearch.sink.SinkBulkResponse;
import com.couchbase.connector.elasticsearch.sink.SinkBulkResponseItem;
import com.couchbase.connector.elasticsearch.sink.SinkErrorCause;
import com.couchbase.connector.elasticsearch.sink.SinkOps;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.annotations.VisibleForTesting;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;

import static com.couchbase.client.core.util.CbCollections.transform;

public class ElasticsearchSinkOps implements SinkOps {
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
  public Version version() throws IOException {
    // Can't do it the easy way due to https://github.com/elastic/elasticsearch-java/issues/346
    // return Version.parseVersion(client.info().version().number());

    // So use low level client instead :-p
    final Response response = lowLevelClient.performRequest(new Request("GET", "/"));
    try (InputStream is = response.getEntity().getContent()) {
      TextNode node = (TextNode) jsonMapper.readTree(is).at("/version/number");
      return Version.parseVersion(node.textValue());
    }
  }

  @VisibleForTesting
  public ElasticsearchClient modernClient() {
    return client;
  }

  @VisibleForTesting
  public RestClient lowLevelClient() {
    return lowLevelClient;
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
  public void close() throws IOException {
    client._transport().close();
  }
}
