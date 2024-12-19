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

import com.couchbase.connector.config.toml.ConfigPosition;
import com.couchbase.connector.config.toml.ConfigTable;
import com.couchbase.connector.util.KeyStoreHelper;
import com.google.common.base.Supplier;
import org.immutables.value.Value;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyStore;
import java.util.Optional;

import static com.couchbase.connector.config.ConfigHelper.readPassword;
import static com.google.common.base.Strings.emptyToNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Value.Immutable
public interface TrustStoreConfig extends Supplier<KeyStore> {
  Logger log = LoggerFactory.getLogger(TrustStoreConfig.class);

  String path();

  @Nullable
  @Value.Redacted
  String password();

  @Nullable
  @Value.Auxiliary
  ConfigPosition position();

  @Value.Lazy
  @Override
  default KeyStore get() {
    return KeyStoreHelper.get(path(), position(), password());
  }

  static Optional<ImmutableTrustStoreConfig> from(ConfigTable config) {
    config.expectOnly("path", "pathToPassword");

    String path = config.getString("path").orElse(null);
    if (isBlank(path) || path.equals("path/to/truststore")) {
      return Optional.empty();
    }

    log.warn("The [truststore] config section is deprecated. Please use the 'pathToCaCertificate' properties instead.");

    final String password = readPassword(config, "truststore", "pathToPassword");

    return Optional.of(
        ImmutableTrustStoreConfig.builder()
            .path(config.getRequiredString("path"))
            .password(emptyToNull(password))
            .position(config.inputPositionOf("path"))
            .build()
    );
  }
}
