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

import com.couchbase.connector.config.ConfigException;
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
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport;
import org.opensearch.client.transport.httpclient5.internal.Node;
import org.opensearch.client.transport.httpclient5.internal.NodeSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;

import javax.net.ssl.SSLContext;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static com.couchbase.client.core.util.CbCollections.transform;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OpenSearchHelper {
  private static final Logger log = LoggerFactory.getLogger(OpenSearchHelper.class);

  private OpenSearchHelper() {
    throw new AssertionError("not instantiable");
  }

  private static Duration socketTimeout(ConnectorConfig config) {
    Duration bulkRequestTimeout = config.elasticsearch().bulkRequest().timeout();
    return Duration.ofMillis(
        Math.max(
            SECONDS.toMillis(60),
            bulkRequestTimeout.toMillis() + SECONDS.toMillis(3)
        )
    );
  }

  private static Duration connectTimeout(ConnectorConfig config) {
    return Duration.ofSeconds(10);
  }

  private static Duration connectionAcquisitionTimeout(ConnectorConfig config) {
    return connectTimeout(config).multipliedBy(2);
  }

  private static Duration connectionIdleTime(ConnectorConfig config) {
    return Duration.ofSeconds(10);
  }

  public static CloseableHttpAsyncClient newHttpClient(ConnectorConfig config) {
    int socketTimeoutMillis = (int) socketTimeout(config).toMillis();
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
        .evictIdleConnections(TimeValue.ofMilliseconds(connectionIdleTime(config).toMillis()))
        .setDefaultCredentialsProvider((authScope, context) -> credentials)
        .setDefaultRequestConfig(RequestConfig.custom()
            .setConnectTimeout(connectTimeout(config).toMillis(), MILLISECONDS)
            .setConnectionRequestTimeout(connectionAcquisitionTimeout(config).toMillis(), MILLISECONDS)
            .build()
        )
        .build();
    client.start();
    return client;

  }

  private static SSLContext sslContext(ConnectorConfig config) {
    try {
      KeyStore trustStore = KeyStoreHelper.trustStoreFrom(
          "OpenSearch",
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

  public static OpenSearchSinkOps newAwsOpenSearchSinkOps(ConnectorConfig config) {
    return new OpenSearchSinkOps(
        new OpenSearchClient(newAwsTransport(config)),
        config.elasticsearch().bulkRequest().timeout()
    );
  }

  private static OpenSearchTransport newAwsTransport(ConnectorConfig config) {
    SdkHttpClient awsHttpClient = ApacheHttpClient.builder()
        .connectionTimeout(connectTimeout(config))
        .connectionAcquisitionTimeout(connectionAcquisitionTimeout(config))
        .socketTimeout(socketTimeout(config))
        .build();

    if (config.elasticsearch().hosts().size() != 1) {
      throw new ConfigException("Must specify exactly one host when connecting to Amazon OpenSearch Service.");
    }

    int port = config.elasticsearch().hosts().get(0).getPort();
    if (port != -1 && port != 443) {
      // AwsSdk2Transport does not support non-standard ports.
      log.warn("Ignoring non-standard port ({}) in Amazon OpenSearch Service address.", port);
    }

    String host = config.elasticsearch().hosts().get(0).getHostName();

    return new AwsSdk2Transport(
        awsHttpClient,
        host,
        Region.of(config.elasticsearch().aws().region()),
        AwsSdk2TransportOptions.builder().build()
    );
  }

  public static OpenSearchSinkOps newOpenSearchSinkOps(
      CloseableHttpAsyncClient httpClient,
      ConnectorConfig config
  ) {
    return new OpenSearchSinkOps(
        new OpenSearchClient(newApacheHttpClientTransport(httpClient, config)),
        config.elasticsearch().bulkRequest().timeout()
    );
  }

  private static OpenSearchTransport newApacheHttpClientTransport(
      CloseableHttpAsyncClient httpClient,
      ConnectorConfig config
  ) {
    List<Node> nodes = transform(config.elasticsearch().hosts(), it -> new Node(toHttpClient5(it)));
    return new ApacheHttpClient5Transport(
        httpClient,
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
  }
}
