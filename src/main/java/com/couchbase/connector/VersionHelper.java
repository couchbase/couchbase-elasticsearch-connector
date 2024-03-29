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
import java.util.Optional;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;

public class VersionHelper {
  private VersionHelper() {
    throw new AssertionError("not instantiable");
  }

  private static final String version;
  private static final String gitInfo;

  static {
    String tempVersion = "unknown";
    String tempGitInfo = "unknown";

    try {
      final String versionResource = "version.properties";
      try (InputStream is = VersionHelper.class.getResourceAsStream(versionResource)) {
        if (is == null) {
          throw new IOException("missing version properties resource: " + versionResource);
        }
        try (Reader r = new InputStreamReader(is, UTF_8)) {
          final Properties props = new Properties();
          props.load(r);
          tempVersion = (String) props.getOrDefault("version", "unknown");
          tempGitInfo = (String) props.getOrDefault("git", "unknown");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    version = tempVersion;
    gitInfo = tempGitInfo;
  }

  public static String getVersionString() {
    final String gitInfo = getGitInfo().orElse(null);
    return gitInfo == null ? version : version + "(" + gitInfo + ")";
  }

  public static String getVersion() {
    return version;
  }

  public static Optional<String> getGitInfo() {
    return version.equals(gitInfo) ? Optional.empty() : Optional.of(gitInfo);
  }
}
