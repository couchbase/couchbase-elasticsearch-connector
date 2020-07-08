/*
 * Copyright 2020 Couchbase, Inc.
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

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ScopeAndCollection {
  private final String scope;
  private final String collection;

  public static ScopeAndCollection parse(String scopeAndCollection) {
    String[] split = scopeAndCollection.split("\\.", -1);
    if (split.length != 2) {
      throw new IllegalArgumentException(
          "Expected qualified collection name (scope.collection) but got: " + scopeAndCollection);
    }
    return new ScopeAndCollection(split[0], split[1]);
  }

  public ScopeAndCollection(String scope, String collection) {
    this.scope = requireNonNull(scope);
    this.collection = requireNonNull(collection);
  }

  public String getScope() {
    return scope;
  }

  public String getCollection() {
    return collection;
  }

  public String format() {
    return scope + "." + collection;
  }

  @Override
  public String toString() {
    return format();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ScopeAndCollection that = (ScopeAndCollection) o;
    return scope.equals(that.scope) &&
        collection.equals(that.collection);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scope, collection);
  }
}
