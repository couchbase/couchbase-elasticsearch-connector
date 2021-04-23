/*
 * Copyright 2021 Couchbase, Inc.
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

import com.couchbase.connector.config.toml.ConfigPosition;
import com.couchbase.connector.config.toml.ConfigTable;
import com.couchbase.connector.util.KeyStoreHelper;
import org.immutables.value.Value;

import javax.annotation.Nullable;
import java.security.KeyStore;

import static com.couchbase.connector.config.ConfigHelper.readPassword;
import static com.google.common.base.Strings.emptyToNull;

@Value.Immutable
public interface ClientCertConfig {

  boolean use();

  String path();

  @Nullable
  @Value.Redacted
  String password();

  @Nullable
  @Value.Auxiliary
  ConfigPosition position();

  @Value.Lazy
  default KeyStore getKeyStore() {
    return KeyStoreHelper.get(path(), position(), password());
  }

  static ImmutableClientCertConfig from(ConfigTable config, String parent) {
    if (config.isEmpty()) {
      return ClientCertConfig.disabled();
    }

    String[] configProps = {"use", "path", "pathToPassword"};
    config.expectOnly(configProps);
    config.require(parent, configProps);

    if (!config.getRequiredBoolean("use")) {
      return ClientCertConfig.disabled();
    }

    return ImmutableClientCertConfig.builder()
        .use(true)
        .path(config.getRequiredString("path"))
        .password(emptyToNull(readPassword(config, parent, "pathToPassword")))
        .position(config.inputPositionOf("path"))
        .build();
  }

  static ImmutableClientCertConfig disabled() {
    return ImmutableClientCertConfig.builder()
        .use(false)
        .password("")
        .path("")
        .build();
  }
}
