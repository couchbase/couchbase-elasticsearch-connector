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

package com.couchbase.connector.cluster.k8s;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

public class StatefulSetInfo {
  private static final Logger log = LoggerFactory.getLogger(StatefulSetInfo.class);

  public final String name;
  public final int podOrdinal;

  public StatefulSetInfo(String name, int podOrdinal) {
    this.name = requireNonNull(name);
    this.podOrdinal = podOrdinal;
  }

  @Override
  public String toString() {
    return "StatefulSetInfo{" +
        "name='" + name + '\'' +
        ", podOrdinal=" + podOrdinal +
        '}';
  }

  /**
   * The hostname of a pod running in a stateful set is the name of the stateful set
   * followed by a hyphen (-) and finally the pod number (zero-based).
   * This method parses the hostname into those parts.
   */
  public static StatefulSetInfo fromHostname() {
    String hostname = System.getenv("HOSTNAME");
    requireNonNull(hostname, "HOSTNAME environment variable not set.");
    try {
      log.debug("HOSTNAME = {}", hostname);
      int separatorIndex = hostname.lastIndexOf("-");
      int ordinal = Integer.parseInt(hostname.substring(separatorIndex + 1));
      String statefulSetName = hostname.substring(0, separatorIndex);
      return new StatefulSetInfo(statefulSetName, ordinal);
    } catch (Exception e) {
      throw new RuntimeException(
          "HOSTNAME environment variable '" + hostname + "' doesn't match <name>-<ordinal>." +
              " Make sure to deploy as StatefulSet.");
    }
  }
}
