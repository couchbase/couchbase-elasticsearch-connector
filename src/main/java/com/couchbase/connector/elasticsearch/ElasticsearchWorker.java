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

import com.couchbase.connector.dcp.Event;
import com.couchbase.connector.elasticsearch.io.ElasticsearchWriter;
import org.elasticsearch.common.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

public class ElasticsearchWorker implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchWorker.class);
  private static final AtomicInteger nameCounter = new AtomicInteger();

  private final Thread thread;
  private final ErrorListener errorHandler;
  private final ElasticsearchWriter writer;
  private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();
  private final BlockingQueue<Throwable> fatalErrorQueue;

  private ElasticsearchWorker(ElasticsearchWriter writer, BlockingQueue<Throwable> fatalErrorQueue, @Nullable ErrorListener errorListener) {
    this.writer = requireNonNull(writer);
    this.errorHandler = errorListener == null ? ErrorListener.NOOP : errorListener;
    this.fatalErrorQueue = requireNonNull(fatalErrorQueue);
    this.thread = new Thread(doRun(), "es-worker-" + nameCounter.getAndIncrement());
    this.thread.setDaemon(true);
  }

  public static ElasticsearchWorker newWorker(ElasticsearchWriter writer, BlockingQueue<Throwable> fatalErrorQueue, @Nullable ErrorListener errorListener) {
    ElasticsearchWorker worker = new ElasticsearchWorker(writer, fatalErrorQueue, errorListener);
    worker.thread.start();
    return worker;
  }

  public void submit(Event event) {
    eventQueue.add(event);
  }

  public int getQueueSize() {
    return eventQueue.size();
  }

  public long getCurrentRequestNanos() {
    return writer.getCurrentRequestNanos();
  }

  private Runnable doRun() {
    return () -> {
      try {
        while (!Thread.interrupted()) {

          // Wait for the next event, then grab as many as are immediately available
          Event event = eventQueue.take();
          writer.write(event);
          while ((event = eventQueue.poll()) != null) {
            writer.write(event);
          }

          writer.flush();
        }

      } catch (Throwable t) {
        if (!isNormalTermination(t)) {
          LOGGER.warn("Error in Elasticsearch worker thread", t);
        }
        fatalErrorQueue.offer(t);

      } finally {
        LOGGER.info("{} stopped.", Thread.currentThread());
      }
    };
  }

  private boolean isNormalTermination(Throwable t) {
    return t instanceof InterruptedException;
  }

  @Override
  public void close() {
    thread.interrupt();
  }

  public boolean join(long millis) throws InterruptedException {
    thread.join(millis);
    return !thread.isAlive();
  }

  public String toString() {
    return thread.toString();
  }
}
