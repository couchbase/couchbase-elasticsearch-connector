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

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.Uninterruptibles.joinUninterruptibly;
import static java.util.Objects.requireNonNull;

/**
 * Helper for running interruptible tasks in a separate thread.
 */
public class AsyncTask implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncTask.class);

  private static final AtomicInteger threadCounter = new AtomicInteger();

  @FunctionalInterface
  public interface Interruptible {
    /**
     * When the thread executing this method is interrupted,
     * the method should throw InterruptedException to indicate clean termination.
     */
    void run() throws Throwable;
  }

  private final Thread thread;
  private final AtomicReference<Throwable> connectorException = new AtomicReference<>();

  public static AsyncTask run(Interruptible task, Consumer<Throwable> fatalErrorListener) {
    return new AsyncTask(task, fatalErrorListener).start();
  }

  public static AsyncTask run(Interruptible task) {
    return run(task, t -> {
    });
  }

  private AsyncTask(Interruptible task, Consumer<Throwable> fatalErrorListener) {
    requireNonNull(task);
    requireNonNull(fatalErrorListener);

    thread = new Thread(() -> {
      try {
        task.run();
      } catch (Throwable t) {
        connectorException.set(t);
        if (t instanceof InterruptedException) {
          LOGGER.debug("Connector task interrupted");
        } else {
          LOGGER.error("Connector task exited early due to failure", t);
          fatalErrorListener.accept(t);
        }
      }
    }, "connector-main-" + threadCounter.getAndIncrement());
  }

  private AsyncTask start() {
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

    Duration timeout = Duration.ofSeconds(30);
    joinUninterruptibly(thread, timeout);
    if (thread.isAlive()) {
      throw new TimeoutException("Connector didn't exit within " + timeout);
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
  public void close() throws IOException {
    try {
      stop();
    } catch (Throwable t) {
      Throwables.propagateIfPossible(t, IOException.class);
      throw new RuntimeException(t);
    }
  }
}
