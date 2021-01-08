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

package com.couchbase.connector.cluster.consul;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.orbitz.consul.model.agent.DebugConfig;
import com.orbitz.consul.util.Jackson;

/**
 * The Consul client we're using has <a href="https://github.com/rickfast/consul-client/issues/431">
 * a bug that causes it to fail when parsing the debug config of Consul 1.8.4 and later</a>.
 * <p>
 * The bug has been patched, but a version with the fix has not yet been released.
 * In the mean time, this workaround prevents the client from parsing the debug config
 * (which we don't use anyway).
 */
public class ConsulClientWorkaround {
  private ConsulClientWorkaround() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Prevent the Consul client from attempting to deserialize the DebugConfig
   * associated with the /v1/agent/self response.
   */
  public static void apply() {
    try {
      // The target class is package-private, so reference it by name.
      Class<?> mixinTarget = Class.forName("com.orbitz.consul.model.agent.ImmutableAgent$Json");
      Jackson.MAPPER.addMixIn(mixinTarget, AgentJsonMixin.class);

    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to apply Consul client debug config workaround.", e);
    }
  }

  private static abstract class AgentJsonMixin {
    @JsonIgnore
    @JsonProperty("DebugConfig")
    public abstract void setDebugConfig(DebugConfig debugConfig);

    @JsonIgnore
    @JsonProperty("DebugConfig")
    public abstract DebugConfig getDebugConfig();
  }
}
