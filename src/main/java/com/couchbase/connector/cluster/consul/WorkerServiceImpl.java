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

import com.couchbase.connector.cluster.Membership;
import com.couchbase.connector.config.common.ImmutableGroupConfig;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.config.es.ImmutableConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class WorkerServiceImpl implements WorkerService {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerServiceImpl.class);

  private volatile Status status = Status.IDLE;

  private volatile ConnectorTask connectorTask;

  private final Consumer<Throwable> fatalErrorListener;

  public WorkerServiceImpl(Consumer<Throwable> fatalErrorListener) {
    this.fatalErrorListener = requireNonNull(fatalErrorListener);
  }

  @Override
  public synchronized void stopStreaming() {
    if (connectorTask != null) {
      try {
        connectorTask.close();
        this.status = Status.IDLE;

      } catch (Throwable t) {
        LOGGER.error("Connector task failed to close. Terminating worker process.", t);
        System.exit(1);

      } finally {
        connectorTask = null;
      }
    }
  }

  @Override
  public synchronized void startStreaming(Membership membership, String config) {
    stopStreaming();

    final ImmutableConnectorConfig originalConfig = ConnectorConfig.from(config);

    // Plug in the appropriate group membership. Ick.
    final ConnectorConfig patchedConfig = originalConfig
        .withGroup(ImmutableGroupConfig.copyOf(originalConfig.group())
            .withStaticMembership(membership));

    connectorTask = new ConnectorTask(patchedConfig, fatalErrorListener).start();

    this.status = new Status(membership);
  }

  @Override
  public synchronized Status status() {
    return status;
  }
}
