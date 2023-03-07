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

import com.couchbase.connector.config.common.ClientCertConfig;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.config.es.ElasticsearchConfig;
import com.couchbase.connector.util.KeyStoreHelper;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.util.TimeValue;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport;
import org.opensearch.client.transport.httpclient5.internal.Node;
import org.opensearch.client.transport.httpclient5.internal.NodeSelector;

import javax.net.ssl.SSLContext;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.util.CbCollections.transform;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OpenSearchHelper {
  private OpenSearchHelper() {
    throw new AssertionError("not instantiable");
  }

  public static CloseableHttpAsyncClient newHttpClient(ConnectorConfig config) {
    Duration bulkRequestTimeout = config.elasticsearch().bulkRequest().timeout();
    final int socketTimeoutMillis = (int) Math.max(
        SECONDS.toMillis(60),
        bulkRequestTimeout.toMillis() + SECONDS.toMillis(3)
    );

    IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
        .setSoTimeout(socketTimeoutMillis, MILLISECONDS)
        .build();

    TlsStrategy tlsStrategy = !config.elasticsearch().secureConnection() ? null : ClientTlsStrategyBuilder.create()
        .setSslContext(sslContext(config))
        .build();

    PoolingAsyncClientConnectionManager cm = PoolingAsyncClientConnectionManagerBuilder.create()
        .setTlsStrategy(tlsStrategy)
        .build();

    UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(
        config.elasticsearch().username(),
        config.elasticsearch().password().toCharArray()
    );

    CloseableHttpAsyncClient client = HttpAsyncClients.custom()
        .setConnectionManager(cm)
        .setIOReactorConfig(ioReactorConfig)
        .evictExpiredConnections()
        .evictIdleConnections(TimeValue.ofSeconds(10))
        .setDefaultCredentialsProvider((authScope, context) -> credentials)
        .setDefaultRequestConfig(RequestConfig.custom()
            .setConnectTimeout(10, TimeUnit.SECONDS)
            .setConnectionRequestTimeout(10, TimeUnit.SECONDS)
            .build()
        )
        .build();
    client.start();
    return client;

  }

  private static SSLContext sslContext(ConnectorConfig config) {
    try {
      KeyStore trustStore = KeyStoreHelper.trustStoreFrom(
          "Elasticsearch",
          config.elasticsearch().caCert(),
          config.trustStore().orElse(null)
      ).get();

      final SSLContextBuilder builder = SSLContexts.custom()
          .loadTrustMaterial(trustStore, null);

      ClientCertConfig clientCert = config.elasticsearch().clientCert();
      if (clientCert.use()) {
        String passwordString = clientCert.password();
        char[] pw = passwordString == null ? null : passwordString.toCharArray();
        try {
          builder.loadKeyMaterial(clientCert.getKeyStore(), pw);
        } finally {
          if (pw != null) {
            Arrays.fill(pw, '\0');
          }
        }
      }

      return builder.build();

    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  private static HttpHost toHttpClient5(org.apache.http.HttpHost host) {
    return new HttpHost(host.getSchemeName(), host.getHostName(), host.getPort());
  }

  public static List<HttpHost> hosts(ElasticsearchConfig config) {
    return transform(config.hosts(), it -> toHttpClient5(it));
  }

  public static OpenSearchSinkOps newOpenSearchClient(CloseableHttpAsyncClient httpClient, ConnectorConfig config) {
    List<Node> nodes = transform(config.elasticsearch().hosts(), it -> new Node(toHttpClient5(it)));
    ApacheHttpClient5Transport transport = new ApacheHttpClient5Transport(httpClient,
        new Header[0],
        nodes,
        new JacksonJsonpMapper(),
        null,
        "",
        new ApacheHttpClient5Transport.FailureListener() {
          public void onFailure(Node node) {
            Metrics.elasticsearchHostOffline().increment();
          }
        },
        NodeSelector.ANY,
        false,
        true,
        true
    );

    OpenSearchClient client = new OpenSearchClient(transport);

    return new OpenSearchSinkOps(
        client,
        config.elasticsearch().bulkRequest().timeout()
    );
  }
}
