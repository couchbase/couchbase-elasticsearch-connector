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

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import com.couchbase.client.dcp.util.Version;
import com.couchbase.connector.config.common.ClientCertConfig;
import com.couchbase.connector.config.common.TrustStoreConfig;
import com.couchbase.connector.config.es.AwsConfig;
import com.couchbase.connector.config.es.ElasticsearchConfig;
import com.couchbase.connector.util.ThrowableHelper;
import com.google.common.collect.Iterables;
import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.couchbase.connector.elasticsearch.io.MoreBackoffPolicies.truncatedExponentialBackoff;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ElasticsearchHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchHelper.class);

  private ElasticsearchHelper() {
    throw new AssertionError("not instantiable");
  }

  private static final Pattern ES_ERROR_TYPE_PATTERN = Pattern.compile("\\[type=(.+?),");

  public static Optional<String> getElasticsearchExceptionType(BulkItemResponse.Failure failure) {
    final Matcher m = ES_ERROR_TYPE_PATTERN.matcher(failure.getMessage());
    if (!m.find()) {
      return Optional.empty();
    }
    return Optional.of(m.group(1));
  }

  public static Version waitForElasticsearchAndRequireVersion(RestHighLevelClient esClient, Version required, Version recommended) throws InterruptedException {
    final Iterator<TimeValue> retryDelays = truncatedExponentialBackoff(
        TimeValue.timeValueSeconds(1), TimeValue.timeValueMinutes(1)).iterator();

    while (true) {
      try {
        org.elasticsearch.Version esVersion = esClient.info().getVersion();
        final Version version = new Version(esVersion.major, esVersion.minor, esVersion.revision);
        if (version.compareTo(required) < 0) {
          throw new RuntimeException("Elasticsearch version " + required + " or later required; actual version is " + version);
        }
        if (version.compareTo(recommended) < 0) {
          LOGGER.warn("Elasticsearch version " + version + " is lower than recommended version " + recommended + ".");
        }
        return version;

      } catch (Exception e) {
        final TimeValue delay = retryDelays.next();
        LOGGER.warn("Failed to connect to Elasticsearch. Retrying in {}", delay, e);
        if (ThrowableHelper.hasCause(e, ConnectionClosedException.class)) {
          LOGGER.warn("  Troubleshooting tip: If the Elasticsearch connection failure persists," +
              " and if Elasticsearch is configured to require TLS/SSL, then make sure the connector is also configured to use secure connections.");
        }

        MILLISECONDS.sleep(delay.millis());
      }
    }
  }

  public static RestHighLevelClient newElasticsearchClient(ElasticsearchConfig elasticsearchConfig, TrustStoreConfig trustStoreConfig) throws Exception {
    return newElasticsearchClient(
        elasticsearchConfig.hosts(),
        elasticsearchConfig.username(),
        elasticsearchConfig.password(),
        elasticsearchConfig.secureConnection(),
        trustStoreConfig,
        elasticsearchConfig.clientCert(),
        elasticsearchConfig.aws(),
        elasticsearchConfig.bulkRequest().timeout());
  }

  private static long toMillis(TimeValue timeValue) {
    return timeValue.timeUnit().toMillis(timeValue.duration());
  }

  public static RestHighLevelClient newElasticsearchClient(List<HttpHost> hosts, String username, String password, boolean secureConnection, Supplier<KeyStore> trustStore, ClientCertConfig clientCert, AwsConfig aws, TimeValue bulkRequestTimeout) throws Exception {
    final int connectTimeoutMillis = (int) SECONDS.toMillis(5);
    final int socketTimeoutMillis = (int) Math.max(SECONDS.toMillis(60), toMillis(bulkRequestTimeout) + SECONDS.toMillis(3));
    LOGGER.info("Elasticsearch client connect timeout = {}ms; socket timeout={}ms", connectTimeoutMillis, socketTimeoutMillis);

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(username, password));

    final SSLContext sslContext = !secureConnection ? null : newSslContext(trustStore.get(), clientCert);

    final RestClientBuilder builder = RestClient.builder(Iterables.toArray(hosts, HttpHost.class))
        .setHttpClientConfigCallback(httpClientBuilder -> {
          httpClientBuilder.setSSLContext(sslContext);
          if (!clientCert.use()) {
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
          }
          awsSigner(aws).ifPresent(httpClientBuilder::addInterceptorLast);
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

    return new RestHighLevelClient(builder);
  }

  private static SSLContext newSslContext(KeyStore trustStore, ClientCertConfig clientCert) throws Exception {
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
  }

  private static Optional<HttpRequestInterceptor> awsSigner(AwsConfig config) {
    if (config.region().isEmpty()) {
      return Optional.empty();
    }

    final String serviceName = "es";

    final AWS4Signer signer = new AWS4Signer();
    signer.setServiceName(serviceName);
    signer.setRegionName(config.region());

    return Optional.of(new AWSRequestSigningApacheInterceptor(
        serviceName, signer, new DefaultAWSCredentialsProviderChain()));
  }

}
