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
import com.google.common.collect.ImmutableList;
import net.consensys.cava.toml.TomlTable;
import org.immutables.value.Value;

import javax.annotation.Nullable;
import java.util.List;

import static com.couchbase.connector.config.ConfigHelper.expectOnly;
import static com.couchbase.connector.config.ConfigHelper.getOptionalList;
import static com.couchbase.connector.config.ConfigHelper.getStrings;
import static com.couchbase.connector.config.ConfigHelper.readPassword;
import static com.google.common.base.Strings.isNullOrEmpty;

@Value.Immutable
public interface CouchbaseConfig {
  List<String> hosts();

  NetworkResolution network();

  String username();

  @Value.Redacted()
  String password();

  String bucket();

  String metadataBucket();

  @Nullable
  String scope();

  @Value.Default
  default List<ScopeAndCollection> collections() {
    return ImmutableList.of();
  }

  boolean secureConnection();

  DcpConfig dcp();

  @Value.Check
  default void check() {
    if (!isNullOrEmpty(scope()) && !collections().isEmpty()) {
      throw new ConfigException("Invalid configuration; you can specify 'scope' OR 'collections', but not both.");
    }
  }

  static ImmutableCouchbaseConfig from(TomlTable config) {
    expectOnly(config, "bucket", "metadataBucket", "scope", "collections", "hosts", "network", "username", "pathToPassword", "dcp", "secureConnection");

    final String sourceBucket = config.getString("bucket", () -> "default");
    final String networkName = config.getString("network", () -> "auto");
    final String metadataBucket = config.getString("metadataBucket", () -> "");

    return ImmutableCouchbaseConfig.builder()
        .bucket(sourceBucket)
        .scope(config.getString("scope"))
        .collections(getOptionalList(config, "collections", ScopeAndCollection::parse))
        .metadataBucket(isNullOrEmpty(metadataBucket) ? sourceBucket : metadataBucket)
        .hosts(getStrings(config, "hosts"))
        .network(networkName.isEmpty() ? NetworkResolution.AUTO : NetworkResolution.custom(networkName))
        .username(config.getString("username", () -> ""))
        .password(readPassword(config, "couchbase", "pathToPassword"))
        .secureConnection(config.getBoolean("secureConnection", () -> false))
        .dcp(DcpConfig.from(config.getTableOrEmpty("dcp")))
        .build();
  }
}
