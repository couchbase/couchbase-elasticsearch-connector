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

import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.connector.config.ConfigException;
import com.couchbase.connector.config.common.TrustStoreConfig;
import com.google.common.hash.Hashing;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.couchbase.connector.config.ConfigHelper.resolveIfRelative;

public class KeyStoreHelper {
  private static final Logger log = LoggerFactory.getLogger(KeyStoreHelper.class);

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

  public static Supplier<KeyStore> trustStoreFrom(
      String serviceName,
      List<X509Certificate> trustedCertificates,
      @Nullable TrustStoreConfig trustStoreConfig
  ) {
    if (trustedCertificates.isEmpty()) {
      if (trustStoreConfig != null) {
        // use the global trust store (deprecated)
        return trustStoreConfig;
      }

      log.info(
          "Since no Certificate Authority (CA) certificate(s) were specified for {}," +
              " the connector will trust the well-known CAs in the JVM's 'cacerts' trust store." +
              " If the {} service uses a private CA, make sure to configure the 'pathToCaCertificate'" +
              " connector config property, or add the CA certificate to `cacerts`.",
          serviceName, serviceName
      );

      trustedCertificates = SecurityConfig.jvmCaCertificates();
    }

    List<X509Certificate> certs = new ArrayList<>(trustedCertificates);
    return () -> {
      try {
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);
        for (int i = 0; i < certs.size(); i++) {
          ks.setCertificateEntry(serviceName + "-" + i, certs.get(i));
        }
        return ks;
      } catch (GeneralSecurityException | IOException e) {
        throw new ConfigException(
            "Failed to generate KeyStore for " + serviceName + " Certificate Authority (CA) certificate.",
            e
        );
      }
    };
  }

  public static String describe(List<X509Certificate> certificates) {
    if (certificates.isEmpty()) {
      return "n/a";
    }

    return certificates.stream()
        .map(it -> "SHA-256 fingerprint: " + sha512(it) + " " + it.getSubjectX500Principal() + " (valid from " + it.getNotBefore().toInstant() + " to " + it.getNotAfter().toInstant() + ")")
        .collect(Collectors.toList())
        .toString();
  }

  private static String sha512(X509Certificate cert) {
    try {
      return Hashing.sha256().hashBytes(cert.getEncoded()).toString();
    } catch (CertificateEncodingException e) {
      throw new RuntimeException(e);
    }
  }

}
