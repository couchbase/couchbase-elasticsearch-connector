/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.consul;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

public class KvReadResult {
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  private final String key;
  private final long flags;
  private final byte[] value;
  private final String session;
  private final long createIndex;
  private final long modifyIndex;
  private final long lockIndex;

  @JsonCreator
  public KvReadResult(
      @JsonProperty("Key") String key,
      @JsonProperty("Flags") long flags,
      @JsonProperty("Value") byte[] value,
      @JsonProperty("Session") String session,
      @JsonProperty("CreateIndex") long createIndex,
      @JsonProperty("ModifyIndex") long modifyIndex,
      @JsonProperty("LockIndex") long lockIndex
  ) {
    this.key = key;
    this.flags = flags;
    this.value = value == null ? EMPTY_BYTE_ARRAY : value;
    this.session = session;
    this.createIndex = createIndex;
    this.modifyIndex = modifyIndex;
    this.lockIndex = lockIndex;
  }

  public String key() {
    return key;
  }

  public long flags() {
    return flags;
  }

  public byte[] value() {
    return value;
  }

  public String valueAsString() {
    return new String(value, UTF_8);
  }

  public Optional<String> session() {
    return Optional.ofNullable(session);
  }

  public long createIndex() {
    return createIndex;
  }

  public long modifyIndex() {
    return modifyIndex;
  }

  public long lockIndex() {
    return lockIndex;
  }

  @Override
  public String toString() {
    return "KvReadResult{" +
        "key='" + key + '\'' +
        ", flags=" + flags +
        ", session='" + session + '\'' +
        ", createIndex=" + createIndex +
        ", modifyIndex=" + modifyIndex +
        ", lockIndex=" + lockIndex +
        ", value=" + valueAsString() +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KvReadResult that = (KvReadResult) o;
    return flags == that.flags && createIndex == that.createIndex && modifyIndex == that.modifyIndex && lockIndex == that.lockIndex && key.equals(that.key) && Arrays.equals(value, that.value) && Objects.equals(session, that.session);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, modifyIndex, lockIndex);
  }
}
