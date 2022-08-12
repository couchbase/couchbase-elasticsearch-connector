/*
 * Copyright 2019 Couchbase, Inc.
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

import com.couchbase.connector.cluster.consul.ConsulDocumentWatcher;
import com.couchbase.consul.ConsulOps;
import com.google.common.net.HostAndPort;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.Network;
import reactor.core.Disposable;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.connector.elasticsearch.AutonomousOpsTest.CONSUL_DOCKER_IMAGE;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConsulDocumentWatcherTest {

  private static final long TIMEOUT_MILLIS = 20_000;

  // true means assume consul is already running on default port, don't bother with Docker
  private static final boolean NATIVE_CONSUL = false;

  private static ConsulCluster consulCluster;
  private static ConsulOps client;
  private static ConsulDocumentWatcher watcher;

  private static final AtomicInteger keyCounter = new AtomicInteger();

  private static String uniqueKey() {
    return "key" + keyCounter.incrementAndGet() + "-" + UUID.randomUUID();
  }

  @BeforeClass
  public static void startConsul() {
    HostAndPort consulAddress;
    if (NATIVE_CONSUL) {
      consulAddress = HostAndPort.fromParts("127.0.0.1", 8500);
    } else {
      consulCluster = new ConsulCluster(CONSUL_DOCKER_IMAGE, 3, Network.newNetwork()).start();
      consulAddress = consulCluster.getHostAndPort(0);
    }
    client = new ConsulOps(consulAddress, ConsulOps.DEFAULT_CONFIG);
    watcher = new ConsulDocumentWatcher(client);
  }

  @AfterClass
  public static void stopConsul() throws IOException {
    client.close();

    if (!NATIVE_CONSUL) {
      consulCluster.stop();
    }
  }

  @Test
  public void watchWithInitialStateAbsent() throws Exception {
    final String key = uniqueKey();


    final BlockingQueue<Optional<String>> result = new LinkedBlockingQueue<>();
    final Disposable watch = watcher.withPollingInterval(ofSeconds(1)).watch(key)
        .doOnNext(result::add)
        .subscribe();

    try {
      // initial state, document does not exist
      assertEquals(Optional.empty(), result.poll(15, SECONDS));

      upsert(key, "");
      assertEquals(Optional.of(""), result.poll(15, SECONDS));

      // sleep to make sure the watcher's wait timeout isn't inserting empty values
      SECONDS.sleep(5);

      upsert(key, "bar");
      assertEquals(Optional.of("bar"), result.poll(15, SECONDS));

      delete(key);
      assertEquals(Optional.empty(), result.poll(15, SECONDS));

      assertNull(result.poll(1, SECONDS));

    } finally {
      watch.dispose();
    }
  }

  @Test
  public void watchWithInitialStatePresent() throws Exception {
    final String key = uniqueKey();

    upsert(key, "exists");

    final BlockingQueue<Optional<String>> result = new LinkedBlockingQueue<>();
    final Disposable watch = watcher.watch(key)
        .doOnNext(result::add)
        .subscribe();
    try {
      // initial state
      assertEquals(Optional.of("exists"), result.poll(15, SECONDS));

      upsert(key, "changed");
      assertEquals(Optional.of("changed"), result.poll(15, SECONDS));

      assertNull(result.poll(1, SECONDS));

      watch.dispose();
      upsert(key, "changed again but no longer watching");
      assertNull(result.poll(1, SECONDS));

    } finally {
      watch.dispose();
    }
  }

  @Test(expected = InterruptedException.class, timeout = TIMEOUT_MILLIS)
  public void awaitAbsenceExternalInterruption() throws Throwable {
    final String key = uniqueKey();

    final AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();
    final Thread t = new Thread(() -> {
      try {
        watcher.awaitAbsence(key);
      } catch (Throwable e) {
        exceptionHolder.set(e);
      }
    });

    upsert(key, "foo");
    t.start();
    SECONDS.sleep(2); // wait for awaitAbsence to start executing
    t.interrupt();
    t.join();
    throwIfPresent(exceptionHolder);
  }

  @Test(expected = InterruptedException.class, timeout = TIMEOUT_MILLIS)
  public void awaitAbsenceThreadAlreadyInterrupted() throws Throwable {
    final String key = uniqueKey();

    final AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();
    final Thread t = new Thread(() -> {
      try {
        Thread.currentThread().interrupt();
        watcher.awaitAbsence(key);
      } catch (Throwable e) {
        exceptionHolder.set(e);
      }
    });

    upsert(key, "foo");
    t.start();
    t.join();
    throwIfPresent(exceptionHolder);
  }

  @Test(timeout = TIMEOUT_MILLIS)
  public void awaitAbsence() throws Throwable {
    final String key = uniqueKey();

    final AtomicReference<Throwable> exception = new AtomicReference<>();
    final AtomicBoolean doneWaiting = new AtomicBoolean();

    final Thread t = new Thread(() -> {
      try {
        watcher.awaitAbsence(key);
        doneWaiting.set(true);
      } catch (Throwable e) {
        e.printStackTrace();
        exception.set(e);
      }
    });

    upsert(key, "foo");

    t.start();
    SECONDS.sleep(3);
    assertFalse(doneWaiting.get());
    delete(key);
    t.join();
    throwIfPresent(exception);
    assertTrue(doneWaiting.get());
  }

  @Test(expected = TimeoutException.class, timeout = TIMEOUT_MILLIS)
  public void awaitConditionWithTimeout() throws Throwable {
    final String key = uniqueKey();

    final AtomicReference<Throwable> exception = new AtomicReference<>();

    final Thread t = new Thread(() -> {
      try {
        watcher.awaitCondition(key, String::toUpperCase, doc -> doc.equals(Optional.of("BAR")), Duration.ofMillis(3));
      } catch (Throwable e) {
        exception.set(e);
      }
    });

    upsert(key, "foo");
    t.start();
    t.join();
    throwIfPresent(exception);
  }

  @Test(expected = TimeoutException.class, timeout = TIMEOUT_MILLIS)
  public void unmatchedEmissionsDoNotExtendTimeout() throws Throwable {
    final String key = uniqueKey();

    final AtomicReference<Throwable> exception = new AtomicReference<>();

    final Thread t = new Thread(() -> {
      try {
        watcher.awaitCondition(key, String::toUpperCase, doc -> doc.equals(Optional.of("BAR")), ofSeconds(5));
      } catch (Throwable e) {
        exception.set(e);
      }
    });

    t.start();

    for (int i = 0; i < 10; i++) {
      SECONDS.sleep(1);
      upsert(key, "foo" + i);
    }
    // this one matches, but it should be too late.
    upsert(key, "bar");

    t.join();
    throwIfPresent(exception);
  }

  @Test(timeout = TIMEOUT_MILLIS)
  public void awaitConditionWithTransformation() throws Throwable {
    final String key = uniqueKey();

    final AtomicReference<Throwable> exception = new AtomicReference<>();
    final AtomicBoolean doneWaiting = new AtomicBoolean();

    final Thread t = new Thread(() -> {
      try {
        Optional<String> result = watcher.awaitCondition(key, String::toUpperCase, doc -> doc.equals(Optional.of("BAR")));
        assertEquals(Optional.of("BAR"), result);
        doneWaiting.set(true);
      } catch (Throwable e) {
        e.printStackTrace();
        exception.set(e);
      }
    });

    t.start();
    SECONDS.sleep(2);
    assertFalse(doneWaiting.get());
    upsert(key, "foo");
    SECONDS.sleep(2);
    assertFalse(doneWaiting.get());

    upsert(key, "bar");
    t.join();
    throwIfPresent(exception);
    assertTrue(doneWaiting.get());
  }

  private static <T extends Throwable> void throwIfPresent(AtomicReference<T> ref) throws T {
    T t = ref.get();
    if (t != null) {
      throw t;
    }
  }

  private void upsert(String key, String value) {
    assertTrue(client.kv().upsertKey(key, value).block().body());
  }

  private void delete(String key) {
    assertTrue(client.kv().deleteKey(key).block().body());
  }
}
