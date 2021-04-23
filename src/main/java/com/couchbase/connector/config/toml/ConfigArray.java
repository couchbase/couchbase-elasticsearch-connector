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

import net.consensys.cava.toml.TomlArray;
import net.consensys.cava.toml.TomlPosition;

import static java.util.Objects.requireNonNull;

public class ConfigArray {
  private final TomlArray wrapped;

  public ConfigArray(TomlArray wrapped) {
    this.wrapped = requireNonNull(wrapped);
  }

  public int size() {
    return wrapped.size();
  }

  public ConfigTable getTable(int i) {
    return new ConfigTable(wrapped.getTable(i));
  }

  public ConfigPosition inputPositionOf(int i) {
    TomlPosition pos = wrapped.inputPositionOf(i);
    return pos == null ? null : new ConfigPosition(pos);
  }
}
