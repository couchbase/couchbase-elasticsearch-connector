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

import com.couchbase.connector.cluster.Membership;
import com.couchbase.connector.config.ConfigException;
import net.consensys.cava.toml.TomlTable;
import org.immutables.value.Value;

import static com.couchbase.connector.config.ConfigHelper.expectOnly;
import static com.couchbase.connector.config.ConfigHelper.getInt;
import static com.couchbase.connector.config.ConfigHelper.getIntInRange;

@Value.Immutable
public interface GroupConfig {
  String name();

  Membership staticMembership();

  static ImmutableGroupConfig from(TomlTable config) {
    expectOnly(config, "name", "static");

    final TomlTable staticGroup = config.getTableOrEmpty("static");
    expectOnly(staticGroup, "memberNumber", "totalMembers");

    final int totalMembers = getIntInRange(staticGroup, "totalMembers", 1, 1024).orElseThrow((() ->
        new ConfigException("missing 'static.totalMembers' property")));

    final int memberNumber = getInt(staticGroup, "memberNumber").orElseThrow(() ->
        new ConfigException("missing 'static.memberNumber' property"));

    try {
      return ImmutableGroupConfig.builder()
          .name(config.getString("name"))
          .staticMembership(Membership.of(memberNumber, totalMembers))
          .build();
    } catch (IllegalArgumentException e) {
      throw new ConfigException(e.getMessage());
    }
  }
}
