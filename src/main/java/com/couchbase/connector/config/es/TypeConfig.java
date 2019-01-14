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

import com.couchbase.client.deps.io.netty.util.internal.StringUtil;
import com.couchbase.connector.config.ConfigException;
import com.couchbase.connector.dcp.Event;
import com.google.common.base.Strings;
import net.consensys.cava.toml.TomlPosition;
import net.consensys.cava.toml.TomlTable;
import org.immutables.value.Value;

import javax.annotation.Nullable;
import java.util.regex.Matcher;
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

  @Nullable
  String routing(); // may be null, only for custom routing

  IndexMatcher indexMatcher();

  @Nullable
  RoutingMatcher routingMatcher(); // may be null, only for custom routing

  @Value.Auxiliary
  @Nullable
  TomlPosition position();

  @Value.Check
  default void check() {
    if (!ignore() && index() == null && !(indexMatcher() instanceof IdRegexInferredIndexMatcher)) {
      throw new ConfigException("Missing 'index' (or 'regex' with capturing group named 'index') for config at " + position());
    }
  }

  static ImmutableTypeConfig from(TomlTable config, TomlPosition position, TypeConfig defaults) {
    expectOnly(config, "typeName", "index", "pipeline", "ignore", "ignoreDeletes", "prefix", "regex", "routing");

    final String index = Strings.emptyToNull(config.getString("index", defaults::index));
    ImmutableTypeConfig.Builder builder = ImmutableTypeConfig.builder()
        .position(position)
        .type(config.getString("typeName", defaults::type))
        .index(index)
        .pipeline(Strings.emptyToNull(config.getString("pipeline", defaults::pipeline)))
        .ignoreDeletes(config.getBoolean("ignoreDeletes", defaults::ignoreDeletes))
        .ignore(config.getBoolean("ignore", defaults::ignore))
        .routing(config.getString("routing", defaults::routing));

    final String idPrefix = config.getString("prefix");
    final String idRegex = config.getString("regex");
    if (idPrefix != null && idRegex != null) {
      throw new ConfigException("Type at " + position + " can have 'prefix' or 'regex', but not both.");
    }
    if (idPrefix == null && idRegex == null) {
      throw new ConfigException("Type at " + position + " must have 'prefix' or 'regex'.");
    }
    if (idPrefix != null) {
      builder.indexMatcher(new IdPrefixMatcher(index, idPrefix));
    } else {
      try {
        if (idRegex.contains("(?<index>")) {
          if (config.getString("index") != null) {
            throw new ConfigException("Type at " + position + " must not have 'index' because it's inferred from named capturing group in 'regex'.");
          }
          builder.indexMatcher(new IdRegexInferredIndexMatcher(idRegex));
        } else {
          builder.indexMatcher(new IdRegexMatcher(index, idRegex));
        }
      } catch (PatternSyntaxException e) {
        throw new ConfigException("Invalid regex '" + idRegex + "' at " + config.inputPositionOf("regex") + " -- " + e.getMessage());
      }
    }

    final String routing = config.getString("routing");
    if (Strings.isNullOrEmpty(routing) == false)
      builder.routingMatcher(new IdRoutingMatcher(routing));

    return builder.build();
  }

  interface IndexMatcher {
    String getIndexIfMatches(Event event);
  }

  interface RoutingMatcher {
    String getRoutingIfMatches(Event event);
  }

  class IdPrefixMatcher implements IndexMatcher {
    private final String prefix;
    private final String index;

    public IdPrefixMatcher(String index, String prefix) {
      this.index = index;
      this.prefix = requireNonNull(prefix);
    }

    @Override
    public String getIndexIfMatches(Event event) {
      return event.getKey().startsWith(prefix) ? index : null;
    }

    @Override
    public String toString() {
      return "prefix='" + prefix + "'";
    }
  }

  class IdRegexMatcher implements IndexMatcher {
    private final String index;
    private final Pattern pattern;

    public IdRegexMatcher(String index, String pattern) {
      this.index = index;
      this.pattern = Pattern.compile(pattern);
    }

    @Override
    public String getIndexIfMatches(Event event) {
      return pattern.matcher(event.getKey()).matches() ? index : null;
    }

    @Override
    public String toString() {
      return "regex='" + pattern + "'";
    }
  }

  class IdRegexInferredIndexMatcher implements IndexMatcher {
    private final Pattern pattern;

    public IdRegexInferredIndexMatcher(String pattern) {
      this.pattern = Pattern.compile(pattern);
    }

    @Override
    public String getIndexIfMatches(Event event) {
      Matcher m = pattern.matcher(event.getKey());
      return m.matches() ? m.group("index") : null;
    }

    @Override
    public String toString() {
      return "regex='" + pattern + "'";
    }
  }

  class IdRoutingMatcher implements RoutingMatcher {
    private final String routing;

    public IdRoutingMatcher(String routing) {
      this.routing = requireNonNull(routing);
    }

    @Override
    public String getRoutingIfMatches(Event event) {
      if (Strings.isNullOrEmpty(event.getKey()) == true)
        return StringUtil.EMPTY_STRING;

      String[] parts = event.getKey().split(this.routing);
      return parts.length > 1 ? parts[1] : StringUtil.EMPTY_STRING;
    }
  }
}
