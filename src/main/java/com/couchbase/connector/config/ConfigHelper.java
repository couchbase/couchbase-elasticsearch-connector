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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import net.consensys.cava.toml.Toml;
import net.consensys.cava.toml.TomlArray;
import net.consensys.cava.toml.TomlParseResult;
import net.consensys.cava.toml.TomlTable;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.couchbase.connector.util.EnvironmentHelper.getInstallDir;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.startsWithIgnoreCase;

public class ConfigHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigHelper.class);

  public static Optional<Integer> getInt(TomlTable toml, String key) {
    return getIntInRange(toml, key, 1, Integer.MAX_VALUE);
  }

  public static Optional<Integer> getIntInRange(TomlTable toml, String key, int min, int max) {
    final Long result = toml.getLong(key);
    if (result == null) {
      return Optional.empty();
    }
    if (result > max) {
      throw new ConfigException("Value for '" + key + "' at " + toml.inputPositionOf(key) + " may not be larger than " + max);
    }
    if (result < min) {
      throw new ConfigException("Value for '" + key + "' at " + toml.inputPositionOf(key) + " may not be smaller than " + min);
    }
    return Optional.of(result.intValue());
  }

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

  public static List<String> getStrings(TomlTable toml, String name) {
    List<String> results = getOptionalList(toml, name, Function.identity());
    if (results.isEmpty()) {
      throw new ConfigException("Config key '" + name + "' must be present, and must have at least one value.");
    }
    return results;
  }

  public static <R> List<R> getOptionalList(TomlTable toml, String name, Function<String, R> transformer) {
    final TomlArray array = toml.getArray(name);
    if (array == null) {
      return ImmutableList.of();
    }
    try {
      return ImmutableList.copyOf(
          array.toList()
              .stream()
              .map(String.class::cast)
              .map(transformer)
              .collect(toList()));
    } catch (ClassCastException e) {
      throw new ConfigException("Array '" + name + "' may only contain strings");
    } catch (RuntimeException e) {
      throw new ConfigException("Failed to parse array '" + name + "'; " + e.getMessage());
    }
  }

  public static Optional<ByteSizeValue> getSize(TomlTable toml, String dottedKey) {
    return Optional.ofNullable(
        ByteSizeValue.parseBytesSizeValue(
            toml.getString(dottedKey),
            "'" + dottedKey + "' at " + toml.inputPositionOf(dottedKey)));
  }

  public static Optional<TimeValue> getTime(TomlTable toml, String dottedKey) {
    // unlike parseBytesSizeValue, the time parser doesn't accept null :-p
    final String value = toml.getString(dottedKey);
    if (value == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(
        TimeValue.parseTimeValue(
            value,
            "'" + dottedKey + "' at " + toml.inputPositionOf(dottedKey)));
  }

  public static void expectOnly(TomlTable table, String... recognizedKeys) {
    final Set<String> recognized = new HashSet<>(Arrays.asList(recognizedKeys));
    final Set<String> present = table.keySet();
    Set<String> unrecognized = Sets.difference(present, recognized);
    if (!unrecognized.isEmpty()) {
      final String key = unrecognized.iterator().next();
      throw new ConfigException("Unrecognized item '" + key + "' at " + table.inputPositionOf(key) +
          "; expected items in this context are: " + Arrays.toString(recognizedKeys));
    }
  }

  public static void require(TomlTable table, String parent, String... requiredKeys) {
    final Set<String> required = new HashSet<>(Arrays.asList(requiredKeys));
    final Set<String> present = table.keySet();
    Set<String> missing = Sets.difference(required, present);
    if (!missing.isEmpty()) {
      final String key = missing.iterator().next();
      throw new ConfigException("Missing config property '" + key + "' for " + parent +
          "; required items in this context are: " + Arrays.toString(requiredKeys));
    }
  }


  public static String readPassword(TomlTable parent, String parentName, String keyName) {
    final String pathToPassword = parent.getString(keyName);
    if (pathToPassword == null) {
      throw new ConfigException(parentName + "." + keyName + " must not be null");
    }

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
