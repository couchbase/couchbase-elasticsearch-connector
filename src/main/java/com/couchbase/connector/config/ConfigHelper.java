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

package com.couchbase.connector.config;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.util.Golang;
import com.couchbase.connector.config.toml.ConfigTable;
import com.google.common.io.ByteStreams;
import org.apache.commons.text.StringSubstitutor;
import org.apache.http.HttpHost;
import org.apache.tuweni.toml.Toml;
import org.apache.tuweni.toml.TomlParseResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.couchbase.connector.util.EnvironmentHelper.getInstallDir;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.startsWithIgnoreCase;

public class ConfigHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigHelper.class);

  public static HttpHost createHttpHost(String s, int defaultPort, boolean secure) {
    if (secure && startsWithIgnoreCase(s, "http:")) {
      throw new ConfigException("Elasticsearch host URL " + s + " uses scheme 'http' which conflicts with 'secureConnection' setting of true. For a secure connection, omit the 'http://' prefix, or use prefix 'https://'.");
    }

    final HttpHost host = HttpHost.create(s);
    if ("https".equals(host.getSchemeName()) && !secure) {
      throw new ConfigException("Elasticsearch host URL " + host + " uses https; must set elasticsearch 'secureConnection' config key to true.");
    }

    int port = host.getPort() == -1 ? defaultPort : host.getPort();
    return new HttpHost(host.getHostName(), port, secure ? "https" : "http");
  }

  public static Optional<StorageSize> getSize(ConfigTable toml, String dottedKey) {
    return toml.getString(dottedKey).map(it -> {
      try {
        return StorageSize.parse(it);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Failed to parse storage size value '" + it + "' from config property '" + dottedKey + "'" +
                " at " + toml.inputPositionOf(dottedKey) + "; " + e.getMessage());
      }
    });
  }

  public static Optional<Duration> getTime(ConfigTable toml, String dottedKey) {
    return toml.getString(dottedKey).map(it -> {
      try {
        return parseTime(it);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Failed to parse duration value '" + it + "' from config property '" + dottedKey + "'" +
                " at " + toml.inputPositionOf(dottedKey) + "; " + e.getMessage());
      }
    });
  }

  static Duration parseTime(String value) {
    // Previous versions of the connector parsed durations using org.elasticsearch.common.unit.TimeValue.
    // Massage the input so values that were recognized by TimeValue can be parsed as valid Golang durations.
    String normalizedValue = value
        .toLowerCase(Locale.ROOT)
        .trim()
        .replaceAll("\\s+(\\D+)", "$1") // remove whitespace before a non-digit
        .replace("nanos", "ns")
        .replace("micros", "us");
    return Golang.parseDuration(normalizedValue);
  }

  public static List<X509Certificate> readCertificates(ConfigTable parent, String parentName, String keyName) {
    final String path = parent.getString(keyName).orElse(null);
    if (isBlank(path)) {
      return emptyList();
    }

    final File file = resolveIfRelative(path);
    String context = "File: '" + file + "' (specified as '" + path + "' at " + parentName + "." + keyName;
    try (InputStream is = new FileInputStream(file)) {
      //noinspection unchecked
      List<X509Certificate> result = List.copyOf((List<X509Certificate>) getX509CertificateFactory().generateCertificates(is));
      if (result.isEmpty()) {
        LOGGER.warn("Certificate file contained zero certificates! " + context);
      }
      return result;

    } catch (IOException e) {
      throw new ConfigException("Failed to read certificate file; " + context, e);

    } catch (CertificateException e) {
      throw new ConfigException("Failed to parse certificate(s); expected one or more X.509 certificates in PEM format; " + context, e);
    }
  }

  private static CertificateFactory getX509CertificateFactory() {
    try {
      return CertificateFactory.getInstance("X.509");
    } catch (CertificateException e) {
      throw new CouchbaseException("Could not instantiate X.509 CertificateFactory", e);
    }
  }

  public static String readPassword(ConfigTable parent, String parentName, String keyName) {
    final String pathToPassword = parent.getString(keyName).orElseThrow(() ->
        new ConfigException(parentName + "." + keyName + " must not be null"));

    final File passwordFile = resolveIfRelative(pathToPassword);
    try (InputStream is = new FileInputStream(passwordFile)) {
      final TomlParseResult config = Toml.parse(resolveVariables(is));

      if (config.hasErrors()) {
        // DO NOT REPORT ERRORS, AS THAT MIGHT LEAK THE CONTENTS OF THE FILE TO AN ATTACKER
        // trying to use the connector's elevated privileges to read a completely
        // unrelated file.
      }

      final String password = config.getString("password");
      if (password == null) {
        throw new ConfigException("Failed to parse " + passwordFile + " : Expected a TOML file with contents like: password = 'swordfish'");
      }
      return password;

    } catch (FileNotFoundException e) {
      throw new ConfigException("Error reading config at " + parent.inputPositionOf(keyName) + "; File not found: " + passwordFile);
    } catch (IOException e) {
      LOGGER.error("Failed to read password from file {}", passwordFile, e);
      throw new ConfigException(e.getMessage());
    }
  }

  public static void warnIfDeprecatedTypeNameIsPresent(ConfigTable config) {
    config.getString("typeName").ifPresent(it -> LOGGER.warn(
        "The `typeName` config property is DEPRECATED, since Elasticsearch no longer has the concept of document types." +
            " Please remove the property from your config file; a future version of the connector will reject it as invalid."
    ));
  }

  public static File resolveIfRelative(String pathToPassword) {
    final File f = new File(pathToPassword);
    return f.isAbsolute() ? f : new File(getInstallDir(), pathToPassword);
  }

  public static String resolveVariables(String s) {
    return new StringSubstitutor(System::getenv)
        .setValueDelimiter(":") // following Elasticsearch convention, default values are delimited by ":"
        .replace(s);
  }

  public static String resolveVariables(InputStream is) throws IOException {
    return resolveVariables(new String(ByteStreams.toByteArray(is), UTF_8));
  }
}
