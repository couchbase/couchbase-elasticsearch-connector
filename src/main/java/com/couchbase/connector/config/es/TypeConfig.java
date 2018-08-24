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

package com.couchbase.connector.config.es;

import com.couchbase.connector.config.ConfigException;
import com.couchbase.connector.dcp.Event;
import com.google.common.base.Strings;
import net.consensys.cava.toml.TomlPosition;
import net.consensys.cava.toml.TomlTable;
import org.immutables.value.Value;

import javax.annotation.Nullable;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.couchbase.connector.config.ConfigHelper.expectOnly;
import static java.util.Objects.requireNonNull;

@Value.Immutable
public interface TypeConfig {
  @Nullable
  String index(); // may be null only for ignored types

  String type();

  @Nullable
  String pipeline();

  boolean ignore();

  boolean ignoreDeletes();

  Predicate<Event> matcher();

  @Value.Auxiliary
  @Nullable
  TomlPosition position();

  @Value.Check
  default void check() {
    if (index() == null && !ignore()) {
      throw new ConfigException("Missing 'index' for type at " + position());
    }
  }

  static ImmutableTypeConfig from(TomlTable config, TomlPosition position, TypeConfig defaults) {
    expectOnly(config, "typeName", "index", "pipeline", "ignore", "ignoreDeletes", "prefix", "regex");

    ImmutableTypeConfig.Builder builder = ImmutableTypeConfig.builder()
        .position(position)
        .type(config.getString("typeName", defaults::type))
        .index(Strings.emptyToNull(config.getString("index", defaults::index)))
        .pipeline(Strings.emptyToNull(config.getString("pipeline", defaults::pipeline)))
        .ignoreDeletes(config.getBoolean("ignoreDeletes", defaults::ignoreDeletes))
        .ignore(config.getBoolean("ignore", defaults::ignore));

    final String idPrefix = config.getString("prefix");
    final String idRegex = config.getString("regex");
    if (idPrefix != null && idRegex != null) {
      throw new ConfigException("Type at " + position + " can have 'prefix' or 'regex', but not both.");
    }
    if (idPrefix == null && idRegex == null) {
      throw new ConfigException("Type at " + position + " must have 'prefix' or 'regex'.");
    }
    if (idPrefix != null) {
      builder.matcher(new IdPrefixMatcher(idPrefix));
    } else {
      try {
        builder.matcher(new IdRegexMatcher(idRegex));
      } catch (PatternSyntaxException e) {
        throw new ConfigException("Invalid regex '" + idRegex + "' at " + config.inputPositionOf("regex") + " -- " + e.getMessage());
      }
    }

    return builder.build();
  }

  class IdPrefixMatcher implements Predicate<Event> {
    private final String prefix;

    public IdPrefixMatcher(String prefix) {
      this.prefix = requireNonNull(prefix);
    }

    @Override
    public boolean test(Event event) {
      return event.getKey().startsWith(prefix);
    }

    @Override
    public String toString() {
      return "prefix='" + prefix + "'";
    }
  }

  class IdRegexMatcher implements Predicate<Event> {
    private final Pattern pattern;

    public IdRegexMatcher(String pattern) {
      this.pattern = Pattern.compile(pattern);
    }

    @Override
    public boolean test(Event event) {
      return pattern.matcher(event.getKey()).matches();
    }

    @Override
    public String toString() {
      return "regex='" + pattern + "'";
    }
  }
}
