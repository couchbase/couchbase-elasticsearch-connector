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

package com.couchbase.connector.cluster.consul;

import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.elasticsearch.ElasticsearchConnector;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ConnectorTask implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorTask.class);

  private static final AtomicInteger threadCounter = new AtomicInteger();

  private final Thread thread;
  private final AtomicReference<Throwable> connectorException = new AtomicReference<>();

  public ConnectorTask(ConnectorConfig config, Consumer<Throwable> fatalErrorListener) {
    requireNonNull(fatalErrorListener);

    thread = new Thread(() -> {
      try {
        ElasticsearchConnector.run(config);
      } catch (Throwable t) {
        connectorException.set(t);
        if (t instanceof InterruptedException) {
          LOGGER.debug("Connector task interrupted", t);
        } else {
          LOGGER.error("Connector task exited early due to failure", t);
          fatalErrorListener.accept(t);
        }
      }
    }, "connector-main-" + threadCounter.getAndIncrement());
  }

  public ConnectorTask start() {
    try {
      thread.start();
      LOGGER.info("Thread {} started.", thread.getName());
      return this;

    } catch (IllegalThreadStateException e) {
      throw new IllegalStateException("May only be started once", e);
    }
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
