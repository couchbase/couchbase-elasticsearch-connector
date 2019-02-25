/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.connector.elasticsearch;

import com.google.common.base.Strings;

import java.net.URI;
import java.net.URISyntaxException;

public class DockerHelper {
  private DockerHelper() {
    throw new AssertionError("not instantiable");
  }

  public static String getDockerHost() {
    final String env = System.getenv("DOCKER_HOST");
    if (Strings.isNullOrEmpty(env)) {
      return "localhost";
    }

    try {
      return env.contains("://") ? new URI(env).getHost() : env;
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
