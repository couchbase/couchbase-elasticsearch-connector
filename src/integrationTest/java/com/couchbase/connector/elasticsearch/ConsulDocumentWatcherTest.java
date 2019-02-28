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
import com.orbitz.consul.Consul;
import com.orbitz.consul.KeyValueClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.Network;
import reactor.core.Disposable;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConsulDocumentWatcherTest {

  private static ConsulCluster consulCluster;

  private static final AtomicInteger keyCounter = new AtomicInteger();

  private static String randomKey() {
    return "key" + keyCounter.incrementAndGet();
  }

  @BeforeClass
  public static void startConsul() {
    consulCluster = new ConsulCluster("consul:1.4.2", 1, Network.newNetwork()).start();
  }

  @AfterClass
  public static void stopConsul() {
    consulCluster.stop();
  }

  @Test
  public void watchWithInitialStateAbsent() throws Exception {
    final String key = randomKey();

    final Consul.Builder clientBuilder = consulCluster.clientBuilder(0);

    final KeyValueClient kv = clientBuilder.build().keyValueClient();

    final BlockingQueue<Optional<String>> result = new LinkedBlockingQueue<>();
    final Disposable watch = new ConsulDocumentWatcher(clientBuilder).watch(key)
        .doOnNext(result::add)
        .subscribe();

    try {
      // initial state, document does not exist
      assertEquals(Optional.empty(), result.poll(15, SECONDS));

      kv.putValue(key, "");
      assertEquals(Optional.of(""), result.poll(15, SECONDS));

      kv.putValue(key, "bar");
      assertEquals(Optional.of("bar"), result.poll(15, SECONDS));

      kv.deleteKey(key);
      assertEquals(Optional.empty(), result.poll(15, SECONDS));

      assertNull(result.poll(1, SECONDS));

    } finally {
      watch.dispose();
    }
  }

  @Test
  public void watchWithInitialStatePresent() throws Exception {
    final String key = randomKey();

    final Consul.Builder clientBuilder = consulCluster.clientBuilder(0);

    final KeyValueClient kv = clientBuilder.build().keyValueClient();
    kv.putValue(key, "exists");

    final BlockingQueue<Optional<String>> result = new LinkedBlockingQueue<>();
    final Disposable watch = new ConsulDocumentWatcher(clientBuilder).watch(key)
        .doOnNext(result::add)
        .subscribe();
    try {
      // initial state
      assertEquals(Optional.of("exists"), result.poll(15, SECONDS));

      kv.putValue(key, "changed");
      assertEquals(Optional.of("changed"), result.poll(15, SECONDS));

      assertNull(result.poll(1, SECONDS));

    } finally {
      watch.dispose();
    }
  }
}
