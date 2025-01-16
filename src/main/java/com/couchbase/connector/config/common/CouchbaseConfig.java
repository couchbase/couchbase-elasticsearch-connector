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

package com.couchbase.connector.config.common;

import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.connector.config.ConfigException;
import com.couchbase.connector.config.ScopeAndCollection;
import com.couchbase.connector.config.toml.ConfigTable;
import com.google.common.collect.ImmutableList;
import org.immutables.value.Value;
import org.jspecify.annotations.Nullable;

import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.couchbase.connector.config.ConfigHelper.readCertificates;
import static com.couchbase.connector.config.ConfigHelper.readPassword;
import static com.google.common.base.Strings.isNullOrEmpty;

@Value.Immutable
public interface CouchbaseConfig {
  enum DefaultCheckpoint {
    ZERO,
    NOW,
  }

  List<String> hosts();

  NetworkResolution network();

  String username();

  @Value.Redacted()
  String password();

  String bucket();

  String metadataBucket();

  @Value.Default
  default ScopeAndCollection metadataCollection() {
    return ScopeAndCollection.DEFAULT;
  }

  @Value.Default
  default DefaultCheckpoint defaultCheckpoint() {
    return DefaultCheckpoint.ZERO;
  }

  @Nullable
  String scope();

  @Value.Default
  default List<ScopeAndCollection> collections() {
    return ImmutableList.of();
  }

  boolean secureConnection();

  // Not secret, but redacted because the toString is too verbose
  @Value.Redacted
  List<X509Certificate> caCert();

  boolean hostnameVerification();

  DcpConfig dcp();

  ClientCertConfig clientCert();

  Map<String, String> env();

  @Value.Check
  default void check() {
    if (!isNullOrEmpty(scope()) && !collections().isEmpty()) {
      throw new ConfigException("Invalid configuration; you can specify 'scope' OR 'collections', but not both.");
    }
  }

  static ImmutableCouchbaseConfig from(ConfigTable config) {
    config.expectOnly("bucket", "metadataBucket", "metadataCollection", "defaultCheckpoint", "scope", "collections", "hosts", "network", "username", "pathToPassword", "clientCertificate", "dcp", "secureConnection", "pathToCaCertificate", "hostnameVerification", "env");

    final String sourceBucket = config.getString("bucket").orElse("default");
    final String networkName = config.getString("network").orElse("auto");
    final String metadataBucket = config.getString("metadataBucket").orElse("");
    final String metadataCollection = config.getString("metadataCollection").orElse("_default._default");

    String parentConfigName = "couchbase";

    return ImmutableCouchbaseConfig.builder()
        .bucket(sourceBucket)
        .scope(config.getString("scope").orElse(null))
        .collections(config.getOptionalList("collections", ScopeAndCollection::parse))
        .metadataBucket(isNullOrEmpty(metadataBucket) ? sourceBucket : metadataBucket)
        .metadataCollection(isNullOrEmpty(metadataCollection) ? ScopeAndCollection.DEFAULT : ScopeAndCollection.parse(metadataCollection))
        .defaultCheckpoint(config.getEnum("defaultCheckpoint", DefaultCheckpoint.class).orElse(DefaultCheckpoint.ZERO))
        .hosts(config.getRequiredStrings("hosts"))
        .network(networkName.isEmpty() ? NetworkResolution.AUTO : NetworkResolution.valueOf(networkName))
        .username(config.getString("username").orElse(""))
        .password(readPassword(config, parentConfigName, "pathToPassword"))
        .secureConnection(config.getBoolean("secureConnection").orElse(false))
        .caCert(readCertificates(config, parentConfigName, "pathToCaCertificate"))
        .hostnameVerification(config.getBoolean("hostnameVerification").orElse(true))
        .dcp(DcpConfig.from(config.getTableOrEmpty("dcp")))
        .clientCert(ClientCertConfig.from(config.getTableOrEmpty("clientCertificate"), "couchbase.clientCertificate"))
        .env(parseEnv(config.getTableOrEmpty("env")))
        .build();
  }

  static Map<String, String> parseEnv(ConfigTable env) {
    Map<String, String> result = new HashMap<>();
    for (String key : env.dottedKeySet()) {
      Object value = env.get(key).orElseThrow(() -> new AssertionError("Where did the key '" + key + "' go?"));
      result.put(key, String.valueOf(value));
    }
    return result;
  }
}
