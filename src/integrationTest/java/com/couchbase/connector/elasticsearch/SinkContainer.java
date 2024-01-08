/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.connector.elasticsearch;

import org.opensearch.testcontainers.OpensearchContainer;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Optional;

public interface SinkContainer extends Closeable {

  static SinkContainer create(ElasticsearchVersionSniffer.Flavor flavor, String version) {

    if (flavor == ElasticsearchVersionSniffer.Flavor.ELASTICSEARCH) {
      return new ElasticsearchSinkContainer(
          new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:" + version)
              .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("container.elasticsearch")))
              .withStartupTimeout(Duration.ofMinutes(5))
      ); // CI Docker host is sloooooowwwwwwww
    }

    return new OpensearchSinkContainer(
        new OpensearchContainer<>("opensearchproject/opensearch:" + version)
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("container.opensearch")))
            .withStartupTimeout(Duration.ofMinutes(5))
    );
  }

  boolean isRunning();

  String getHttpHostAddress();

  String username();

  String password();

  Optional<byte[]> caCertAsBytes();

  void start();

  String getDockerImageName();
}
