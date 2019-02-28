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
import com.couchbase.connector.elasticsearch.ElasticsearchConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class WorkerServiceImpl implements WorkerService, Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerServiceImpl.class);

  private volatile Status status = Status.IDLE;

  private volatile AsyncTask connectorTask;

  private final Consumer<Throwable> fatalErrorListener;

  private ScheduledFuture killSwitch;

  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

  public WorkerServiceImpl(Consumer<Throwable> fatalErrorListener) {
    this.fatalErrorListener = requireNonNull(fatalErrorListener);
  }

  public synchronized void resetKillSwitchTimer() {
    if (killSwitch != null) {
      LOGGER.debug("Resetting kill switch.");
      stopKillSwitchTimer();
      startKillSwitchTimer();
    }
  }

  public synchronized void stopKillSwitchTimer() {
    if (killSwitch != null) {
      boolean cancelledSuccessfully = killSwitch.cancel(false);
      killSwitch = null;
    }
  }

  public synchronized void startKillSwitchTimer() {
    checkState(killSwitch == null, "kill switch timer already active");
    LOGGER.debug("Starting kill switch timer.");

    this.killSwitch = executor.schedule(() -> {
      LOGGER.error("Failed to renew Consul session; terminating.");
      System.exit(1); // todo go into "degraded" mode and try to recover instead of exiting
    }, 20, TimeUnit.SECONDS);
  }

  @Override
  public synchronized void stopStreaming() {
    stopKillSwitchTimer();

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

    startKillSwitchTimer();

    final ImmutableConnectorConfig originalConfig = ConnectorConfig.from(config);

    // Plug in the appropriate group membership. Ick.
    final ConnectorConfig patchedConfig = originalConfig
        .withGroup(ImmutableGroupConfig.copyOf(originalConfig.group())
            .withStaticMembership(membership));

    connectorTask = AsyncTask.run(() -> ElasticsearchConnector.run(patchedConfig), fatalErrorListener);

    this.status = new Status(membership);
  }

  @Override
  public synchronized Status status() {
    return status;
  }

  @Override
  public void close() {
    stopStreaming();
    executor.shutdownNow();
  }
}
