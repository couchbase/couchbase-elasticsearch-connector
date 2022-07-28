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
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.couchbase.client.dcp.util.Version;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.IOException;
import java.io.InputStream;

public class ElasticsearchOpsImpl implements ElasticsearchOps {
  private final ElasticsearchClient client;
  private final RestClient lowLevelClient;

  private static final JsonMapper jsonMapper = new JsonMapper();

  public ElasticsearchOpsImpl(RestClientBuilder restClientBuilder) {
    this.lowLevelClient = restClientBuilder.build();

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

  @Override
  public ElasticsearchClient modernClient() {
    return client;
  }

  @Override
  public RestClient lowLevelClient() {
    return lowLevelClient;
  }

  @Override
  public void close() throws IOException {
    client._transport().close();
  }
}
