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

import net.consensys.cava.toml.TomlPosition;

import static java.util.Objects.requireNonNull;

public class ConfigPosition {
  private final TomlPosition wrapped;

  public ConfigPosition(TomlPosition wrapped) {
    this.wrapped = requireNonNull(wrapped);
  }

  @Override
  public String toString() {
    return wrapped.toString();
  }
}
