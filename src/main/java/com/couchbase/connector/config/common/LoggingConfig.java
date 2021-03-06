/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connector.config.common;

import com.couchbase.client.core.logging.RedactionLevel;
import com.couchbase.connector.config.ConfigException;
import com.couchbase.connector.config.ConfigHelper;
import net.consensys.cava.toml.TomlTable;
import org.immutables.value.Value;

import static com.couchbase.connector.config.ConfigHelper.expectOnly;

@Value.Immutable
public interface LoggingConfig {

  boolean logDocumentLifecycle();

  RedactionLevel redactionLevel();

  static ImmutableLoggingConfig from(TomlTable config) {
    expectOnly(config, "logDocumentLifecycle", "redactionLevel");

    try {
      return ImmutableLoggingConfig.builder()
          .logDocumentLifecycle(config.getBoolean("logDocumentLifecycle", () -> false))
          .redactionLevel(ConfigHelper.getEnum(config, "redactionLevel", RedactionLevel.class).orElse(RedactionLevel.NONE))
          .build();
    } catch (IllegalArgumentException e) {
      throw new ConfigException(e.getMessage());
    }
  }
}
