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

package com.couchbase.connector.config.es;

import com.couchbase.connector.config.ConfigException;
import com.couchbase.connector.config.common.ClientCertConfig;
import com.couchbase.connector.config.toml.ConfigArray;
import com.couchbase.connector.config.toml.ConfigTable;
import com.google.common.collect.ImmutableList;
import org.apache.http.HttpHost;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.cert.X509Certificate;
import java.util.List;

import static com.couchbase.connector.config.ConfigHelper.createHttpHost;
import static com.couchbase.connector.config.ConfigHelper.readCertificates;
import static com.couchbase.connector.config.ConfigHelper.readPassword;
import static com.couchbase.connector.config.ConfigHelper.warnIfDeprecatedTypeNameIsPresent;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.removeEnd;

@Value.Immutable
public interface ElasticsearchConfig {
  ImmutableList<HttpHost> hosts();

  String username();

  @Value.Redacted
  String password();

  boolean secureConnection();

  // Not secret, but redacted because the toString is too verbose
  @Value.Redacted
  List<X509Certificate> caCert();

  ClientCertConfig clientCert();

  BulkRequestConfig bulkRequest();

  DocStructureConfig docStructure();

  ImmutableList<TypeConfig> types();

  RejectLogConfig rejectLog();

  ElasticCloudConfig elasticCloud();

  AwsConfig aws();

  @Value.Check
  default void check() {
    if (types().isEmpty()) {
      throw new ConfigException("Must declare at least one [[elasticsearch.type]]");
    }
  }

  static ImmutableElasticsearchConfig from(ConfigTable config) {
    config.expectOnly("hosts", "username", "pathToPassword", "secureConnection", "pathToCaCertificate", "clientCertificate", "elasticCloud", "aws", "bulkRequestLimits", "docStructure", "typeDefaults", "type", "rejectionLog");

    final ElasticCloudConfig elasticCloud = ElasticCloudConfig.from(config.getTableOrEmpty("elasticCloud"));
    final AwsConfig aws = AwsConfig.from(config.getTableOrEmpty("aws"));
    final ClientCertConfig clientCert = ClientCertConfig.from(config.getTableOrEmpty("clientCertificate"), "elasticsearch.clientCertificate");

    final boolean isCloudService = elasticCloud.enabled() || !aws.region().isEmpty();
    final boolean secureConnection = config.getBoolean("secureConnection").orElse(false);

    if (isCloudService && !secureConnection) {
      throw new ConfigException("The Elasticsearch service specified in the connector config requires `secureConnection = true` in the [elasticsearch] config section.");
    }

    // Standard ES HTTP(S) port is 9200. Elastic Cloud and Amazon OpenSearch Service listen on port 443.
    final int defaultPort = isCloudService ? 443 : 9200;

    String parentConfigName = "elasticsearch";
    final ImmutableElasticsearchConfig.Builder builder = ImmutableElasticsearchConfig.builder()
        .secureConnection(secureConnection)
        .caCert(readCertificates(config, parentConfigName, "pathToCaCertificate"))
        .hosts(config.getRequiredStrings("hosts").stream()
            // If copied from AWS dashboard, the "host" might have an irksome trailing slash
            // that interferes with signature validation and hostname sniffing.
            .map(h -> removeEnd(h, "/"))
            .map(h -> createHttpHost(h, defaultPort, secureConnection))
            .collect(toList()))
        .username(config.getString("username").orElse(""))
        .password(readPassword(config, parentConfigName, "pathToPassword"))
        .bulkRequest(BulkRequestConfig.from(config.getTableOrEmpty("bulkRequestLimits")))
        .elasticCloud(elasticCloud)
        .aws(aws)
        .clientCert(clientCert)
        .docStructure(DocStructureConfig.from(config.getTableOrEmpty("docStructure")));

    final ConfigTable typeDefaults = config.getTableOrEmpty("typeDefaults");
    typeDefaults.expectOnly("typeName", "index", "pipeline", "ignore", "ignoreDeletes", "matchOnQualifiedKey");

    warnIfDeprecatedTypeNameIsPresent(typeDefaults);

    final TypeConfig defaultTypeConfig = ImmutableTypeConfig.builder()
        .index(typeDefaults.getString("index").orElse(null))
        .pipeline(typeDefaults.getString("pipeline").orElse(null))
        .ignore(typeDefaults.getBoolean("ignore").orElse(false))
        .ignoreDeletes(typeDefaults.getBoolean("ignoreDeletes").orElse(false))
        .matchOnQualifiedKey(typeDefaults.getBoolean("matchOnQualifiedKey").orElse(false))
        .matcher(s -> null)
        .build();

    ImmutableList.Builder<TypeConfig> typeConfigs = ImmutableList.builder();
    ConfigArray types = config.getArrayOrEmpty("type");
    for (int i = 0; i < types.size(); i++) {
      typeConfigs.add(TypeConfig.from(types.getTable(i), types.inputPositionOf(i), defaultTypeConfig));
    }
    builder.types(typeConfigs.build());

    builder.rejectLog(RejectLogConfig.from(config.getTableOrEmpty("rejectionLog")));

    ImmutableElasticsearchConfig result = builder.build();
    warnIfConfigIsSuspect(result);
    return result;
  }

  private static void warnIfConfigIsSuspect(ElasticsearchConfig config) {
    Logger log = LoggerFactory.getLogger(ElasticsearchConfig.class);

    String firstHostname = config.hosts().get(0).getHostName();

    boolean looksLikeAws = firstHostname.endsWith(".es.amazonaws.com");
    if (looksLikeAws && config.aws().region().isEmpty()) {
      log.warn(
          "It looks like you might be trying to connect to an Amazon OpenSearch Service domain." +
              " If so, please specify the domain's AWS region in the connector's [elasticsearch.aws] config section." +
              " If you are not connecting to an Amazon OpenSearch Service domain, please ignore this message."
      );
    }

    boolean looksLikeElasticCloud = firstHostname.endsWith(".cloud.es.io");
    if (looksLikeElasticCloud && !config.elasticCloud().enabled()) {
      log.warn(
          "It looks like you might be trying to connect to an Elastic Cloud Elasticsearch endpoint." +
              " If so, please set 'enabled = true' in the connector's [elasticsearch.elasticCloud] config section," +
              " and use your Elastic Cloud API key as the password." +
              " If you are not connecting to an Elastic Cloud Elasticsearch endpoint, please ignore this message."
      );
    }
  }
}
