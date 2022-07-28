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
import com.couchbase.connector.config.toml.ConfigPosition;
import com.couchbase.connector.config.toml.ConfigTable;
import com.couchbase.connector.dcp.Event;
import com.fasterxml.jackson.core.JsonPointer;
import com.google.common.base.Strings;
import org.immutables.value.Value;

import javax.annotation.Nullable;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.couchbase.connector.config.ConfigHelper.warnIfDeprecatedTypeNameIsPresent;
import static java.util.Objects.requireNonNull;

@Value.Immutable
public interface TypeConfig {
  @Nullable
  String index(); // may be null only for ignored types, or types where index is inferred using a regex.

  @Nullable
  String pipeline();

  @Nullable
  JsonPointer routing();

  boolean ignore();

  boolean ignoreDeletes();

  boolean matchOnQualifiedKey();

  IndexMatcher matcher();

  @Value.Auxiliary
  @Nullable
  ConfigPosition position();

  @Value.Check
  default void check() {
    if (!ignore() && index() == null && !(matcher() instanceof IdRegexInferredIndexMatcher)) {
      throw new ConfigException("Missing 'index' (or 'regex' with capturing group named 'index') for config at " + position());
    }
  }

  static ImmutableTypeConfig from(ConfigTable config, ConfigPosition position, TypeConfig defaults) {
    config.expectOnly("typeName", "index", "pipeline", "routing", "ignore", "ignoreDeletes", "prefix", "regex", "matchOnQualifiedKey");

    warnIfDeprecatedTypeNameIsPresent(config);

    final String index = Strings.emptyToNull(config.getString("index").orElseGet(defaults::index));
    final String routing = Strings.emptyToNull(config.getString("routing").orElse(null));
    final boolean qualifiedKey = config.getBoolean("matchOnQualifiedKey").orElseGet(defaults::matchOnQualifiedKey);

    ImmutableTypeConfig.Builder builder = ImmutableTypeConfig.builder()
        .position(position)
        .index(index)
        .matchOnQualifiedKey(qualifiedKey)
        .routing(parseRouting(routing, config.inputPositionOf("routing")))
        .pipeline(Strings.emptyToNull(config.getString("pipeline").orElseGet(defaults::pipeline)))
        .ignoreDeletes(config.getBoolean("ignoreDeletes").orElseGet(defaults::ignoreDeletes))
        .ignore(config.getBoolean("ignore").orElseGet(defaults::ignore));

    final String idPrefix = config.getString("prefix").orElse(null);
    final String idRegex = config.getString("regex").orElse(null);
    if (idPrefix != null && idRegex != null) {
      throw new ConfigException("Type at " + position + " can have 'prefix' or 'regex', but not both.");
    }
    if (idPrefix == null && idRegex == null) {
      throw new ConfigException("Type at " + position + " must have 'prefix' or 'regex'.");
    }
    if (idPrefix != null) {
      builder.matcher(new IdPrefixMatcher(index, idPrefix, qualifiedKey));
    } else {
      try {
        if (idRegex.contains("(?<index>")) {
          if (config.getString("index").isPresent()) {
            throw new ConfigException("Type at " + position + " must not have 'index' because it's inferred from named capturing group in 'regex'.");
          }
          builder.matcher(new IdRegexInferredIndexMatcher(idRegex, qualifiedKey));
        } else {
          builder.matcher(new IdRegexMatcher(index, idRegex, qualifiedKey));
        }
      } catch (PatternSyntaxException e) {
        throw new ConfigException("Invalid regex '" + idRegex + "' at " + config.inputPositionOf("regex") + " -- " + e.getMessage());
      }
    }

    final ImmutableTypeConfig type = builder.build();
    if (type.routing() != null && !type.ignoreDeletes()) {
      throw new ConfigException("Custom 'routing' requires 'ignoreDeletes = true' for type at " + position + "." +
          " (Due to limitations in the current implementation, routing information is not available to the connector" +
          " when documents are deleted, so it's not possible to route the deletion request to the correct Elasticsearch shard.)");
    }

    return type;
  }

  static JsonPointer parseRouting(String routing, ConfigPosition position) {
    try {
      return routing == null ? null : JsonPointer.compile(routing);
    } catch (IllegalArgumentException e) {
      throw new ConfigException("Invalid 'routing' JSON pointer at " + position + " ; " + e.getMessage());
    }
  }

  interface IndexMatcher {
    String getIndexIfMatches(Event event);
  }

  class IdPrefixMatcher implements IndexMatcher {
    private final String prefix;
    private final String index;
    private final boolean qualifiedKey;

    public IdPrefixMatcher(String index, String prefix, boolean qualifiedKey) {
      this.index = index;
      this.prefix = requireNonNull(prefix);
      this.qualifiedKey = qualifiedKey;
    }

    @Override
    public String getIndexIfMatches(Event event) {
      return event.getKey(qualifiedKey).startsWith(prefix) ? index : null;
    }

    @Override
    public String toString() {
      return "prefix='" + prefix + "'; qualifiedKey=" + qualifiedKey;
    }
  }

  class IdRegexMatcher implements IndexMatcher {
    private final String index;
    private final Pattern pattern;
    private final boolean qualifiedKey;

    public IdRegexMatcher(String index, String pattern, boolean qualifiedKey) {
      this.index = index;
      this.pattern = Pattern.compile(pattern);
      this.qualifiedKey = qualifiedKey;
    }

    @Override
    public String getIndexIfMatches(Event event) {
      return pattern.matcher(event.getKey(qualifiedKey)).matches() ? index : null;
    }

    @Override
    public String toString() {
      return "regex='" + pattern + "'; qualifiedKey=" + qualifiedKey;
    }
  }

  class IdRegexInferredIndexMatcher implements IndexMatcher {
    private final Pattern pattern;
    private final boolean qualifiedKey;

    public IdRegexInferredIndexMatcher(String pattern, boolean qualifiedKey) {
      this.pattern = Pattern.compile(pattern);
      this.qualifiedKey = qualifiedKey;
    }

    @Override
    public String getIndexIfMatches(Event event) {
      Matcher m = pattern.matcher(event.getKey(qualifiedKey));
      return m.matches() ? sanitizeIndexName(m.group("index")) : null;
    }

    private String sanitizeIndexName(String index) {
      if (index.startsWith("_") || index.startsWith("-")) {
        index = "@" + index.substring(1);
      }
      return index.toLowerCase(Locale.ROOT);
    }

    @Override
    public String toString() {
      return "regex='" + pattern + "'; qualifiedKey=" + qualifiedKey;
    }
  }
}
