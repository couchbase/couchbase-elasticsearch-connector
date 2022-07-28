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

import java.security.cert.X509Certificate;
import java.util.List;

import static com.couchbase.connector.config.ConfigHelper.createHttpHost;
import static com.couchbase.connector.config.ConfigHelper.readCertificates;
import static com.couchbase.connector.config.ConfigHelper.readPassword;
import static com.couchbase.connector.config.ConfigHelper.warnIfDeprecatedTypeNameIsPresent;
import static java.util.stream.Collectors.toList;

@Value.Immutable
public interface ElasticsearchConfig {
  ImmutableList<HttpHost> hosts();

  String username();

  @Value.Redacted
  String password();

  boolean secureConnection();

  @Value.Redacted // not secret, but the toString is too verbose
  List<X509Certificate> caCert();

  ClientCertConfig clientCert();

  BulkRequestConfig bulkRequest();

  DocStructureConfig docStructure();

  ImmutableList<TypeConfig> types();

  RejectLogConfig rejectLog();

  AwsConfig aws();

  @Value.Check
  default void check() {
    if (types().isEmpty()) {
      throw new ConfigException("Must declare at least one [[elasticsearch.type]]");
    }
  }

  static ImmutableElasticsearchConfig from(ConfigTable config) {
    config.expectOnly("hosts", "username", "pathToPassword", "secureConnection", "pathToCaCertificate", "clientCertificate", "aws", "bulkRequestLimits", "docStructure", "typeDefaults", "type", "rejectionLog");

    final boolean secureConnection = config.getBoolean("secureConnection").orElse(false);

    final AwsConfig aws = AwsConfig.from(config.getTableOrEmpty("aws"));
    final ClientCertConfig clientCert = ClientCertConfig.from(config.getTableOrEmpty("clientCertificate"), "elasticsearch.clientCertificate");

    // Standard ES HTTP port is 9200. Amazon Elasticsearch Service listens on ports 80 & 443.
    final int defaultPort = aws.region().isEmpty() ? 9200 :
        secureConnection ? 443 : 80;
    String parentConfigName = "elasticsearch";
    final ImmutableElasticsearchConfig.Builder builder = ImmutableElasticsearchConfig.builder()
        .secureConnection(secureConnection)
        .caCert(readCertificates(config, parentConfigName, "pathToCaCertificate"))
        .hosts(config.getRequiredStrings("hosts").stream()
            .map(h -> createHttpHost(h, defaultPort, secureConnection))
            .collect(toList()))
        .username(config.getString("username").orElse(""))
        .password(readPassword(config, parentConfigName, "pathToPassword"))
        .bulkRequest(BulkRequestConfig.from(config.getTableOrEmpty("bulkRequestLimits")))
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
    return builder.build();

  }
}
