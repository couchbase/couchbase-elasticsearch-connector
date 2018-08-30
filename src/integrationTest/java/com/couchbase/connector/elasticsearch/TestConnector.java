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

package com.couchbase.connector.elasticsearch;

import com.couchbase.connector.config.es.ConnectorConfig;
import com.google.common.base.Throwables;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;

class TestConnector implements AutoCloseable {
  private final Thread thread;
  private final AtomicReference<Throwable> connectorException = new AtomicReference<>();

  public TestConnector(ConnectorConfig config) {
    thread = new Thread(() -> {
      try {
        ElasticsearchConnector.run(config);
      } catch (Throwable t) {
        if (!(t instanceof InterruptedException)) {
          t.printStackTrace();
        }
        connectorException.set(t);
      }
    });
  }

  public TestConnector start() {
    thread.start();
    return this;
  }

  public void stop() throws Throwable {
    thread.interrupt();
    thread.join(SECONDS.toMillis(30));
    if (thread.isAlive()) {
      throw new TimeoutException("Connector didn't exit in allotted time");
    }
    Throwable t = connectorException.get();
    if (t == null) {
      throw new IllegalStateException("Connector didn't exit by throwing exception");
    }
    if (!(t instanceof InterruptedException)) {
      // The connector failed before we asked it to exit!
      throw t;
    }
  }

  @Override
  public void close() throws Exception {
    try {
      stop();
    } catch (Throwable t) {
      Throwables.propagateIfPossible(t, Exception.class);
      throw new RuntimeException(t);
    }
  }
}
