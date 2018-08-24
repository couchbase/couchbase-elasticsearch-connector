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

package com.couchbase.connector;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;

public class VersionHelper {
  private VersionHelper() {
    throw new AssertionError("not instantiable");
  }

  private static final String versionString;

  static {
    String tempVersion = "<unknown>";
    try {
      tempVersion = loadVersionString();
    } catch (Exception e) {
      e.printStackTrace();
    }
    versionString = tempVersion;
  }

  public static String getVersionString() {
    return versionString;
  }

  private static String loadVersionString() throws IOException {
    final String versionResource = "version.properties";
    try (InputStream is = VersionHelper.class.getResourceAsStream(versionResource)) {
      if (is == null) {
        throw new IOException("missing version properties resource: " + versionResource);
      }
      try (Reader r = new InputStreamReader(is, UTF_8)) {
        final Properties props = new Properties();
        props.load(r);
        final String version = props.getProperty("version");
        final String git = props.getProperty("git");
        return version.equals(git) ? version : version + " (" + git + ")";
      }
    }
  }
}
