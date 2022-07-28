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

package com.couchbase.connector.config;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StorageSize {
  private static final Pattern DIGITS = Pattern.compile("\\d+");

  @Override
  public String toString() {
    return bytes + " bytes";
  }

  private final long bytes;

  private StorageSize(long bytes) {
    this.bytes = bytes;
  }

  public static StorageSize ofBytes(long value) {
    return new StorageSize(value);
  }

  public static StorageSize ofMebibytes(long value) {
    return new StorageSize(value * 1024 * 1024);
  }

  public long getBytes() {
    return bytes;
  }

  public static StorageSize parse(String value) {
    try {
      String normalized = value
          .trim()
          .toLowerCase(Locale.ROOT);

      Matcher m = DIGITS.matcher(normalized);
      if (!m.find()) {
        throw new IllegalArgumentException("Value does not start with a number.");
      }
      String number = m.group();

      long num = Long.parseLong(number);

      String unit = normalized.substring(number.length()).trim();
      if (unit.isEmpty()) {
        throw new IllegalArgumentException("Missing size unit.");
      }

      final long scale;
      switch (unit) {
        case "b":
          scale = 1;
          break;
        case "k":
        case "kb":
          scale = 1024L;
          break;
        case "m":
        case "mb":
          scale = 1024L * 1024;
          break;
        case "g":
        case "gb":
          scale = 1024L * 1024 * 1024;
          break;
        case "t":
        case "tb":
          scale = 1024L * 1024 * 1024 * 1024;
          break;
        case "p":
        case "pb":
          scale = 1024L * 1024 * 1024 * 1024 * 1024;
          break;
        default:
          throw new IllegalArgumentException("Unrecognized size unit: '" + unit + "'");
      }

      return new StorageSize(Math.multiplyExact(num, scale));

    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to parse storage size value '" + value + "'; " + e.getMessage() +
              " ; A valid value is a number followed by a unit (for example, '10mb')." +
              " Valid units are" +
              " 'b' for bytes" +
              ", 'k' or 'kb' for kilobytes" +
              ", 'm' or 'mb' for megabytes" +
              ", 'g' or 'gb' for gigabytes" +
              ", 't' or 'tb' for terabytes" +
              ", 'p' or 'pb' for petabytes" +
              "." +
              " Values must be >= 0 bytes and <= " + Long.MAX_VALUE + " bytes.");
    }
  }
}
