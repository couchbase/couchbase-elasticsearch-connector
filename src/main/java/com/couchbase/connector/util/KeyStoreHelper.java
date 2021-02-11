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

package com.couchbase.connector.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.Arrays;

import static com.couchbase.connector.config.ConfigHelper.resolveIfRelative;

public class KeyStoreHelper {
  private KeyStoreHelper() {
    throw new AssertionError("not instantiable");
  }

  public static KeyStore get(String path, Object position, String password) {
    char[] pw = password == null ? null : password.toCharArray();
    try {
      final File keyStoreFile = resolveIfRelative(path);
      if (!keyStoreFile.exists()) {
        throw new RuntimeException("Trust/Key store file configured at " + position + " not found",
            new FileNotFoundException(keyStoreFile.toString()));
      }

      try (InputStream is = new FileInputStream(keyStoreFile)) {
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(is, pw);
        return ks;
      } catch (Exception e) {
        throw new RuntimeException("Failed to read trust/key store file " + keyStoreFile + " configured at " + position + "; " + e.getMessage(), e);
      }
    } finally {
      if (pw != null) {
        Arrays.fill(pw, '\0');
      }
    }
  }
}
