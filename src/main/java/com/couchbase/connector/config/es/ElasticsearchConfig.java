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
import com.google.common.collect.ImmutableList;
import net.consensys.cava.toml.TomlArray;
import net.consensys.cava.toml.TomlTable;
import org.apache.http.HttpHost;
import org.immutables.value.Value;

import static com.couchbase.connector.config.ConfigHelper.readPassword;
import static com.couchbase.connector.config.ConfigHelper.createHttpHost;
import static com.couchbase.connector.config.ConfigHelper.expectOnly;
import static com.couchbase.connector.config.ConfigHelper.getStrings;
import static java.util.stream.Collectors.toList;

@Value.Immutable
public interface ElasticsearchConfig {
  ImmutableList<HttpHost> hosts();

  String username();

  @Value.Redacted
  String password();

  boolean secureConnection();

  BulkRequestConfig bulkRequest();

  DocStructureConfig docStructure();

  ImmutableList<TypeConfig> types();

  RejectLogConfig rejectLog();

  @Value.Check
  default void check() {
    if (types().isEmpty()) {
      throw new ConfigException("Must declare at least one [[elasticsearch.type]]");
    }
  }

  static ImmutableElasticsearchConfig from(TomlTable config) {
    expectOnly(config, "hosts", "username", "pathToPassword", "secureConnection", "bulkRequestLimits", "docStructure", "typeDefaults", "type", "rejectionLog");

    final boolean secureConnection = config.getBoolean("secureConnection", () -> false);

    final ImmutableElasticsearchConfig.Builder builder = ImmutableElasticsearchConfig.builder()
        .secureConnection(secureConnection)
        .hosts(getStrings(config, "hosts").stream()
            .map(h -> createHttpHost(h, 9200, secureConnection))
            .collect(toList()))
        .username(config.getString("username", () -> ""))
        .password(readPassword(config, "elasticsearch", "pathToPassword"))
        .bulkRequest(BulkRequestConfig.from(config.getTableOrEmpty("bulkRequestLimits")))
        .docStructure(DocStructureConfig.from(config.getTableOrEmpty("docStructure")));

    final TomlTable typeDefaults = config.getTableOrEmpty("typeDefaults");
    expectOnly(typeDefaults, "typeName", "index", "pipeline", "ignore", "ignoreDeletes");

    final TypeConfig defaultTypeConfig = ImmutableTypeConfig.builder()
        .index(typeDefaults.getString("index"))
        .type(typeDefaults.getString("typeName", () -> "_doc"))
        .pipeline(typeDefaults.getString("pipeline"))
        .ignore(typeDefaults.getBoolean("ignore", () -> false))
        .ignoreDeletes(typeDefaults.getBoolean("ignoreDeletes", () -> false))
        .matcher(s -> true)
        .build();

    ImmutableList.Builder<TypeConfig> typeConfigs = ImmutableList.builder();
    TomlArray types = config.getArrayOrEmpty("type");
    for (int i = 0; i < types.size(); i++) {
      typeConfigs.add(TypeConfig.from(types.getTable(i), types.inputPositionOf(i), defaultTypeConfig));
    }
    builder.types(typeConfigs.build());

    builder.rejectLog(RejectLogConfig.from(config.getTableOrEmpty("rejectionLog"), defaultTypeConfig.type()));
    return builder.build();

  }
}
