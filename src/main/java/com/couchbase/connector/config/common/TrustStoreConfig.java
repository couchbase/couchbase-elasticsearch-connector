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

import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import net.consensys.cava.toml.TomlPosition;
import net.consensys.cava.toml.TomlTable;
import org.immutables.value.Value;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.security.KeyStore;
import java.util.Arrays;

import static com.couchbase.connector.config.ConfigHelper.expectOnly;
import static com.couchbase.connector.config.ConfigHelper.readPassword;
import static com.couchbase.connector.config.ConfigHelper.resolveIfRelative;
import static com.google.common.base.Throwables.throwIfUnchecked;

@Value.Immutable
public interface TrustStoreConfig extends Supplier<KeyStore> {
  String path();

  @Nullable
  @Value.Redacted
  char[] password();

  @Nullable
  @Value.Auxiliary
  TomlPosition position();

  @Value.Lazy
  @Override
  default KeyStore get() {
    try {
      final File keyStoreFile = resolveIfRelative(path());
      if (!keyStoreFile.exists()) {
        throw new FileNotFoundException("Trust/Key store file configured at " + position() + " not found: " + keyStoreFile);
      }

      KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
      ks.load(new FileInputStream(keyStoreFile), password());
      final char[] array = password();
      if (array != null) {
        // For the security crowd...
        Arrays.fill(array, '*');
      }
      return ks;

    } catch (Exception e) {
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  static ImmutableTrustStoreConfig from(TomlTable config) {
    expectOnly(config, "path", "pathToPassword");

    final String password = readPassword(config, "truststore", "pathToPassword");

    return ImmutableTrustStoreConfig.builder()
        .path(config.getString("path"))
        .password(Strings.isNullOrEmpty(password) ? null : password.toCharArray())
        .position(config.inputPositionOf("path"))
        .build();
  }
}
