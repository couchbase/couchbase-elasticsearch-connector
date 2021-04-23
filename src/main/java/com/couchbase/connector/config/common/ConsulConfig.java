/*
 * Copyright 2019 Couchbase, Inc.
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

import com.couchbase.connector.config.toml.ConfigTable;
import com.couchbase.connector.config.toml.Toml;
import org.immutables.value.Value;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.couchbase.connector.config.ConfigHelper.resolveVariables;

@Value.Immutable
public interface ConsulConfig {
  String aclToken();

  static ImmutableConsulConfig from(ConfigTable root) {
    root.expectOnly("consul");
    ConfigTable config = root.getTableOrEmpty("consul");

    config.expectOnly("aclToken");
    return ImmutableConsulConfig.builder()
        .aclToken(config.getString("aclToken").orElse(""))
        .build();
  }

  static ImmutableConsulConfig from(String toml) {
    return ConsulConfig.from(Toml.parse(resolveVariables(toml)));
  }

  static ImmutableConsulConfig from(InputStream toml) throws IOException {
    return ConsulConfig.from(Toml.parse(resolveVariables(toml)));
  }

  static ImmutableConsulConfig from(File toml) throws IOException {
    try (InputStream is = new FileInputStream(toml)) {
      return ConsulConfig.from(is);
    }
  }
}
