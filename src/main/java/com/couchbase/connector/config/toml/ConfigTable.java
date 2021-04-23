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

package com.couchbase.connector.config.toml;

import com.couchbase.connector.config.ConfigException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import net.consensys.cava.toml.TomlArray;
import net.consensys.cava.toml.TomlPosition;
import net.consensys.cava.toml.TomlTable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class ConfigTable {
  private final TomlTable wrapped;

  public ConfigTable(TomlTable wrapped) {
    this.wrapped = requireNonNull(wrapped);
  }

  public ConfigTable getTableOrEmpty(String key) {
    return new ConfigTable(wrapped.getTableOrEmpty(key));
  }

  public ConfigArray getArrayOrEmpty(String key) {
    return new ConfigArray(wrapped.getArrayOrEmpty(key));
  }

  public Set<String> dottedKeySet() {
    return wrapped.dottedKeySet();
  }

  public ConfigPosition inputPositionOf(String key) {
    TomlPosition pos = wrapped.inputPositionOf(key);
    return pos == null ? null : new ConfigPosition(pos);
  }

  public Optional<?> get(String key) {
    return Optional.ofNullable(wrapped.get(key));
  }

  public boolean isEmpty() {
    return wrapped.isEmpty();
  }

  public Optional<String> getString(String key) {
    return Optional.ofNullable(wrapped.getString(key));
  }

  public String getRequiredString(String key) {
    return getString(key).orElseThrow(() -> new ConfigException("missing '" + key + "' config property"));
  }

  public boolean getRequiredBoolean(String key) {
    return getBoolean(key).orElseThrow(() -> new ConfigException("missing '" + key + "' config property"));
  }

  public Optional<Boolean> getBoolean(String key) {
    try {
      return Optional.ofNullable(wrapped.getBoolean(key));
    } catch (Exception e) {
      return getString(key).map(v -> {
        // let's be stricter than Boolean.parseBoolean()
        switch (v) {
          case "true":
            return Boolean.TRUE;
          case "false":
            return Boolean.FALSE;
          default:
            throw new ConfigException("Value of '" + key + "' could not be parsed as an boolean (must be 'true' or 'false' but got '" + v + "')");
        }
      });
    }
  }

  public Optional<Integer> getInt(String key) {
    return getIntInRange(key, 1, Integer.MAX_VALUE);
  }

  public Optional<Long> getLong(String key) {
    try {
      return Optional.ofNullable(wrapped.getLong(key));
    } catch (Exception e) {
      try {
        return getString(key).map(Long::valueOf);
      } catch (NumberFormatException n) {
        throw new ConfigException("Value of '" + key + "' could not be parsed as an integer; " + n);
      }
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T extends Enum<T>> Optional<T> getEnum(String keyName, Class<T> enumClass) {
    String s = wrapped.getString(keyName);
    if (s == null || s.isEmpty()) {
      return Optional.empty();
    }
    try {
      return Optional.of((T) Enum.valueOf((Class) enumClass, s));
    } catch (IllegalArgumentException e) {
      throw new ConfigException("Unrecognized value '" + s + "' for '" + keyName + "' at " + inputPositionOf(keyName)
          + "; expected one of " + Arrays.toString(enumClass.getEnumConstants()));
    }
  }

  public Optional<Integer> getIntInRange(String key, int min, int max) {
    final Long result = getLong(key).orElse(null);
    if (result == null) {
      return Optional.empty();
    }
    if (result > max) {
      throw new ConfigException("Value for '" + key + "' at " + inputPositionOf(key) + " may not be larger than " + max);
    }
    if (result < min) {
      throw new ConfigException("Value for '" + key + "' at " + inputPositionOf(key) + " may not be smaller than " + min);
    }
    return Optional.of(result.intValue());
  }

  public void expectOnly(String... recognizedKeys) {
    final Set<String> recognized = new HashSet<>(Arrays.asList(recognizedKeys));
    final Set<String> present = wrapped.keySet();
    Set<String> unrecognized = Sets.difference(present, recognized);
    if (!unrecognized.isEmpty()) {
      final String key = unrecognized.iterator().next();
      throw new ConfigException("Unrecognized item '" + key + "' at " + inputPositionOf(key) +
          "; expected items in this context are: " + Arrays.toString(recognizedKeys));
    }
  }

  public void require(String parent, String... requiredKeys) {
    final Set<String> required = new HashSet<>(Arrays.asList(requiredKeys));
    final Set<String> present = wrapped.keySet();
    Set<String> missing = Sets.difference(required, present);
    if (!missing.isEmpty()) {
      final String key = missing.iterator().next();
      throw new ConfigException("Missing config property '" + key + "' for " + parent +
          "; required items in this context are: " + Arrays.toString(requiredKeys));
    }
  }

  public <R> List<R> getOptionalList(String name, Function<String, R> transformer) {
    final TomlArray array = wrapped.getArray(name);
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

  public List<String> getRequiredStrings(String name) {
    List<String> results = getOptionalList(name, Function.identity());
    if (results.isEmpty()) {
      throw new ConfigException("Config key '" + name + "' must be present, and must have at least one value.");
    }
    return results;
  }
}
