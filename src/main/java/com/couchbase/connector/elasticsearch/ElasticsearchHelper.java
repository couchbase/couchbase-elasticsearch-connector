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

import com.codahale.metrics.Gauge;
import com.couchbase.client.core.logging.RedactableArgument;
import com.couchbase.client.java.util.features.Version;
import com.couchbase.connector.config.common.TrustStoreConfig;
import com.couchbase.connector.config.es.ElasticsearchConfig;
import com.couchbase.connector.util.ThrowableHelper;
import com.google.common.collect.Iterables;
import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.ConnectException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.couchbase.connector.elasticsearch.io.MoreBackoffPolicies.truncatedExponentialBackoff;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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

  @SuppressWarnings("unchecked")
  public static Gauge<String> registerElasticsearchVersionGauge(RestHighLevelClient esClient) {
    return Metrics.gauge("elasticsearchVersion", () -> () -> {
      final MainResponse info = getServerInfo(esClient);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Elasticsearch info: {}", formatServerInfo(info));
      }
      return info.getVersion().toString();
    });
  }

  private static MainResponse getServerInfo(RestHighLevelClient esClient) {
    try {
      return esClient.info();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static String formatServerInfo(MainResponse r) {
    Map<String, Object> esInfo = new LinkedHashMap<>();
    esInfo.put("version", r.getVersion().toString());
    esInfo.put("build", r.getBuild());
    esInfo.put("available", r.isAvailable());
    esInfo.put("clusterName", RedactableArgument.system(r.getClusterName()));
    esInfo.put("clusterUuid", RedactableArgument.system(r.getClusterUuid()));
    esInfo.put("nodeName", RedactableArgument.system(r.getNodeName()));
    return esInfo.toString();
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

  public static RestHighLevelClient newElasticsearchClient(ElasticsearchConfig elasticsearchConfig, TrustStoreConfig trustStoreConfig) throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
    return newElasticsearchClient(
        elasticsearchConfig.hosts(),
        elasticsearchConfig.username(),
        elasticsearchConfig.password(),
        elasticsearchConfig.secureConnection(),
        trustStoreConfig);
  }

  public static RestHighLevelClient newElasticsearchClient(List<HttpHost> hosts, String username, String password, boolean secureConnection, Supplier<KeyStore> trustStore) throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(username, password));

    final SSLContext sslContext = !secureConnection ? null :
        SSLContexts.custom().loadTrustMaterial(trustStore.get(), null).build();

    final RestClientBuilder builder = RestClient.builder(Iterables.toArray(hosts, HttpHost.class))
        .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
            .setSSLContext(sslContext)
            .setDefaultCredentialsProvider(credentialsProvider))
        .setFailureListener(new RestClient.FailureListener() {
          @Override
          public void onFailure(HttpHost host) {
            Metrics.elasticsearchHostOffline().mark();
          }
        });

    return new RestHighLevelClient(builder);
  }

}
