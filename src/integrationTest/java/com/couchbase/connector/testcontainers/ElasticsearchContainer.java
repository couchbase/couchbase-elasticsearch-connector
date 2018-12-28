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

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.testcontainers.containers.GenericContainer;

import static java.util.Objects.requireNonNull;

public class ElasticsearchContainer extends GenericContainer<ElasticsearchContainer> {
  private final Version version;

  public ElasticsearchContainer(Version version) {
    super("docker.elastic.co/elasticsearch/elasticsearch:" + requireNonNull(version));
    this.version = version;
  }

  @Override
  protected void configure() {
    addExposedPort(9200);
    if (!version.before(Version.V_5_4_0)) {
      addEnv("discovery.type", "single-node");
    }
  }

  public HttpHost getHost() {
    return new HttpHost("localhost", getMappedPort(9200));
  }
}
