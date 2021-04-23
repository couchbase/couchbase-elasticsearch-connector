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

import com.couchbase.connector.config.toml.ConfigTable;
import com.google.common.io.ByteStreams;
import net.consensys.cava.toml.Toml;
import net.consensys.cava.toml.TomlParseResult;
import org.apache.commons.text.StringSubstitutor;
import org.apache.http.HttpHost;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static com.couchbase.connector.util.EnvironmentHelper.getInstallDir;
import static java.nio.charset.StandardCharsets.UTF_8;
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

  public static Optional<ByteSizeValue> getSize(ConfigTable toml, String dottedKey) {
    return Optional.ofNullable(
        ByteSizeValue.parseBytesSizeValue(
            toml.getString(dottedKey).orElse(null),
            "'" + dottedKey + "' at " + toml.inputPositionOf(dottedKey)));
  }

  public static Optional<TimeValue> getTime(ConfigTable toml, String dottedKey) {
    // unlike parseBytesSizeValue, the time parser doesn't accept null :-p
    final String value = toml.getString(dottedKey).orElse(null);
    if (value == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(
        TimeValue.parseTimeValue(
            value,
            "'" + dottedKey + "' at " + toml.inputPositionOf(dottedKey)));
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
