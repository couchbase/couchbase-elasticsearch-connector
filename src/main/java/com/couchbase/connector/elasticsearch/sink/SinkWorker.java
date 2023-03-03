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

package com.couchbase.connector.elasticsearch.sink;

import com.couchbase.connector.dcp.Event;
import com.couchbase.connector.elasticsearch.ErrorListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

public class SinkWorker implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SinkWorker.class);
  private static final AtomicInteger nameCounter = new AtomicInteger();

  private final Thread thread;
  private final ErrorListener errorHandler;
  private final SinkWriter writer;
  private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();
  private final BlockingQueue<Throwable> fatalErrorQueue;

  private SinkWorker(SinkWriter writer, BlockingQueue<Throwable> fatalErrorQueue, @Nullable ErrorListener errorListener) {
    this.writer = requireNonNull(writer);
    this.errorHandler = errorListener == null ? ErrorListener.NOOP : errorListener;
    this.fatalErrorQueue = requireNonNull(fatalErrorQueue);
    this.thread = new Thread(doRun(), "es-worker-" + nameCounter.getAndIncrement());
    this.thread.setDaemon(true);
  }

  /**
   * @param writer The worker assumes ownership of the writer and is responsible for closing it.
   */
  public static SinkWorker newWorker(SinkWriter writer, BlockingQueue<Throwable> fatalErrorQueue, @Nullable ErrorListener errorListener) {
    SinkWorker worker = new SinkWorker(writer, fatalErrorQueue, errorListener);
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
        drainAndRelease(eventQueue);
        writer.close();
        LOGGER.info("{} stopped.", Thread.currentThread());
      }
    };
  }

  private static void drainAndRelease(BlockingQueue<Event> drainMe) {
    List<Event> releaseMe = new ArrayList<>(drainMe.size());
    drainMe.drainTo(releaseMe);
    releaseMe.forEach(Event::release);
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
