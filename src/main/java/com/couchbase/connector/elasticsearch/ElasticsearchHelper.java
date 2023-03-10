/*
 * Copyright 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connector.elasticsearch;

import com.couchbase.connector.config.common.ClientCertConfig;
import com.couchbase.connector.config.common.TrustStoreConfig;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.config.es.ElasticsearchConfig;
import com.couchbase.connector.util.KeyStoreHelper;
import com.google.common.collect.Iterables;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ElasticsearchHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchHelper.class);

  private ElasticsearchHelper() {
    throw new AssertionError("not instantiable");
  }

  public static ElasticsearchSinkOps newElasticsearchSinkOps(ConnectorConfig config) {
    ElasticsearchConfig elasticsearchConfig = config.elasticsearch();
    TrustStoreConfig trustStoreConfig = config.trustStore().orElse(null);

    Supplier<KeyStore> trustStoreSupplier = KeyStoreHelper.trustStoreFrom(
        "Elasticsearch",
        elasticsearchConfig.caCert(),
        trustStoreConfig
    );

    return new ElasticsearchSinkOps(newRestClient(
        elasticsearchConfig.hosts(),
        elasticsearchConfig.username(),
        elasticsearchConfig.password(),
        elasticsearchConfig.secureConnection(),
        trustStoreSupplier,
        elasticsearchConfig.clientCert(),
        elasticsearchConfig.bulkRequest().timeout()),
        elasticsearchConfig.bulkRequest().timeout()
    );
  }

  public static RestClientBuilder newRestClient(List<HttpHost> hosts, String username, String password, boolean secureConnection, Supplier<KeyStore> trustStore, ClientCertConfig clientCert, Duration bulkRequestTimeout) {
    final int connectTimeoutMillis = (int) SECONDS.toMillis(5);
    final int socketTimeoutMillis = (int) Math.max(SECONDS.toMillis(60), bulkRequestTimeout.toMillis() + SECONDS.toMillis(3));
    LOGGER.info("Elasticsearch client connect timeout = {}ms; socket timeout={}ms", connectTimeoutMillis, socketTimeoutMillis);

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(username, password));

    final SSLContext sslContext = !secureConnection ? null : newSslContext(trustStore.get(), clientCert);

    return RestClient.builder(Iterables.toArray(hosts, HttpHost.class))
        .setHttpClientConfigCallback(httpClientBuilder -> {
          httpClientBuilder.setSSLContext(sslContext);
          if (!clientCert.use()) {
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
          }
          return httpClientBuilder;
        })
        .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
            .setConnectTimeout(connectTimeoutMillis)
            .setSocketTimeout(socketTimeoutMillis)
        )
        .setFailureListener(new RestClient.FailureListener() {
          @Override
          public void onFailure(Node host) {
            Metrics.elasticsearchHostOffline().increment();
          }
        });
  }

  private static SSLContext newSslContext(KeyStore trustStore, ClientCertConfig clientCert) {
    try {
      SSLContextBuilder builder = SSLContexts.custom()
          .loadTrustMaterial(trustStore, null);

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
}
