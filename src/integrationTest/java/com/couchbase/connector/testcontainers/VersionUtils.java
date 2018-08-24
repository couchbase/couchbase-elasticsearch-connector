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

package com.couchbase.connector.testcontainers;

import com.couchbase.client.dcp.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

import static com.couchbase.connector.testcontainers.ExecUtils.exec;

class VersionUtils {
  private static final Logger log = LoggerFactory.getLogger(VersionUtils.class);

  private VersionUtils() {
    throw new AssertionError("not instantiable");
  }

  static Optional<Version> getVersion(CouchbaseContainer couchbase) throws IOException, InterruptedException {
    ExecUtils.ExecResultWithExitCode execResult = exec(couchbase, "couchbase-server --version");
    if (execResult.getExitCode() != 0) {
      return getVersionFromDockerImageName(couchbase);
    }
    Optional<Version> result = tryParseVersion(execResult.getStdout().trim());
    return result.isPresent() ? result : getVersionFromDockerImageName(couchbase);
  }

  private static int indexOfFirstDigit(final String s) {
    for (int i = 0; i < s.length(); i++) {
      if (Character.isDigit(s.charAt(i))) {
        return i;
      }
    }
    return -1;
  }

  private static Optional<Version> tryParseVersion(final String versionString) {
    try {
      // We get a string like "Couchbase Server 5.5.0-2036 (EE)". The version parser
      // tolerates trailing garbage, but not leading garbage, so...
      final int actualStartIndex = indexOfFirstDigit(versionString);
      if (actualStartIndex == -1) {
        return Optional.empty();
      }
      final String versionWithoutLeadingGarbage = versionString.substring(actualStartIndex);
      final Version version = Version.parseVersion(versionWithoutLeadingGarbage);
      // builds off master branch might have version 0.0.0 :-(
      return version.major() == 0 ? Optional.empty() : Optional.of(version);
    } catch (Exception e) {
      log.warn("Failed to parse version string '{}'", versionString, e);
      return Optional.empty();
    }
  }

  private static Optional<Version> getVersionFromDockerImageName(CouchbaseContainer couchbase) {
    final String imageName = couchbase.getDockerImageName();
    final int tagDelimiterIndex = imageName.indexOf(':');
    return tagDelimiterIndex == -1 ? Optional.empty() : tryParseVersion(imageName.substring(tagDelimiterIndex + 1));
  }
}
