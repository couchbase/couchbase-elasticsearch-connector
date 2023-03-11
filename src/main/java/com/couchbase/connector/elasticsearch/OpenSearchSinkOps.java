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

import com.couchbase.connector.elasticsearch.sink.Operation;
import com.couchbase.connector.elasticsearch.sink.SinkBulkResponse;
import com.couchbase.connector.elasticsearch.sink.SinkBulkResponseItem;
import com.couchbase.connector.elasticsearch.sink.SinkErrorCause;
import com.couchbase.connector.elasticsearch.sink.SinkOps;
import com.couchbase.connector.elasticsearch.sink.SinkTestOps;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import jakarta.json.stream.JsonGenerator;
import jakarta.json.stream.JsonParser;
import org.jetbrains.annotations.Nullable;
import org.opensearch.client.ResponseException;
import org.opensearch.client.json.JsonpDeserializer;
import org.opensearch.client.json.JsonpDeserializerBase;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.CountResponse;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.opensearch.core.InfoRequest;
import org.opensearch.client.opensearch.core.MgetResponse;
import org.opensearch.client.opensearch.core.mget.MultiGetResponseItem;
import org.opensearch.client.transport.Endpoint;
import org.opensearch.client.transport.endpoints.SimpleEndpoint;

import java.io.IOException;
import java.io.StringWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.couchbase.client.core.util.CbCollections.transform;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("DuplicatedCode") // Same code as ElasticsearchSinkOps, but uses classes from different packages.
public class OpenSearchSinkOps implements SinkOps, SinkTestOps {
  private final OpenSearchClient client;

  private final String bulkRequestTimeoutString;

  public OpenSearchSinkOps(OpenSearchClient client, Duration bulkRequestTimeout) {
    this.client = requireNonNull(client);
    this.bulkRequestTimeoutString = bulkRequestTimeout.toMillis() + "ms";
  }

  @Override
  public ObjectNode info() {
    // client.info() does not work with the versions of Elasticsearch hosted on Amazon OpenSearch service.
    // For maximum compatibility with both OpenSearch and Elasticsearch, make the request "manually".

    try {
      JsonpDeserializer<ObjectNode> deserializer = new JsonpDeserializerBase<>(EnumSet.allOf(JsonParser.Event.class)) {
        @Override
        public ObjectNode deserialize(JsonParser parser, JsonpMapper mapper, JsonParser.Event event) {
          return mapper.deserialize(parser, ObjectNode.class);
        }
      };

      Endpoint<InfoRequest, ObjectNode, ErrorResponse> infoEndpoint = new SimpleEndpoint<>(
          request -> "GET", // method
          request -> "/", // path
          request -> emptyMap(), // parameters
          SimpleEndpoint.emptyMap(), // headers
          false, // has request body?
          deserializer
      );

      return client._transport()
          .performRequest(InfoRequest._INSTANCE, infoEndpoint, null);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static final JsonpMapper jsonpMapper = new JacksonJsonpMapper();

  @Override
  public SinkBulkResponse bulk(List<Operation> operations) throws IOException {
    OpenSearchBulkRequestBuilder requestBuilder = new OpenSearchBulkRequestBuilder();
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
                StringWriter w = new StringWriter();
                JsonGenerator generator = jsonpMapper.jsonProvider().createGenerator(w);
                it.error().serialize(generator, jsonpMapper);
                generator.close();
                return w.toString();
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

  @Override
  public Optional<JsonNode> getDocument(String index, String id, @Nullable String routing) {
    try {
      GetResponse<JsonNode> response = client.get(req -> {
        req.index(index)
            .id(id);
        if (routing != null) {
          req.routing(routing);
        }
        return req;
      }, JsonNode.class);

      if (!response.found()) {
        return Optional.empty();
      }

      JsonNode doc = requireNonNull(response.source());

      // The client strips out the "_routing" field (and probably others).
      // Put it back, because the tests were written to look for it.
      String responseRouting = response.routing();
      if (responseRouting != null) {
        ((ObjectNode) doc).set("_routing", new TextNode(responseRouting));
      }
      return Optional.of(doc);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long countDocuments(String index) {
    try {
      CountResponse response = client.count(req -> req.index(index));
      return response.count();

    } catch (ResponseException e) {
      return -1;
    } catch (Exception e) {
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
}
