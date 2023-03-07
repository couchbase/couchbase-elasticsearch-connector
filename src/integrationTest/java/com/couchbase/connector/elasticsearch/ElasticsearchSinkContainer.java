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

import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.io.IOException;
import java.util.Optional;

public class ElasticsearchSinkContainer implements SinkContainer {
  private final ElasticsearchContainer wrapped;

  public ElasticsearchSinkContainer(ElasticsearchContainer wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public boolean isRunning() {
    return wrapped.isRunning();
  }

  @Override
  public String getHttpHostAddress() {
    return wrapped.getHttpHostAddress();
  }

  @Override
  public String username() {
    return "elastic";
  }

  @Override
  public String password() {
    return "changeme";
  }

  @Override
  public Optional<byte[]> caCertAsBytes() {
    return wrapped.caCertAsBytes();
  }

  @Override
  public void start() {
    wrapped.start();
  }

  @Override
  public String getDockerImageName() {
    return wrapped.getDockerImageName();
  }

  @Override
  public void close() throws IOException {
    wrapped.close();
  }
}
