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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import com.codahale.metrics.json.MetricsModule;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.JvmAttributeGaugeSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.couchbase.client.dcp.metrics.DefaultDropwizardConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.dropwizard.DropwizardConfig;
import io.micrometer.core.instrument.dropwizard.DropwizardMeterRegistry;

import java.lang.management.ManagementFactory;
import java.util.Map;

import static com.couchbase.client.dcp.metrics.DefaultDropwizardConfig.PRETTY_TAGS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Provides static access to a shared metrics registry.
 */
public class Metrics {
  private static final MetricRegistry registry = new MetricRegistry();
  private static final String PREFIX = "cbes.";

  static {
    registerAll("jvm.attr", new JvmAttributeGaugeSet());
    registerAll("jvm.mem", new MemoryUsageGaugeSet());
    registerAll("jvm.gc", new GarbageCollectorMetricSet());
    // registerAll("jvm.class", new ClassLoadingGaugeSet());
    registerAll("jvm.buffer", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
    registerAll("jvm.thread", new ThreadStatesGaugeSet());
    registerAll("jvm.cpu", new CpuGaugeSet());
  }

  static {
    // export DCP metrics from Micrometer to the Dropwizard registry
    DropwizardConfig config = new DefaultDropwizardConfig();
    io.micrometer.core.instrument.Metrics.addRegistry(new DropwizardMeterRegistry(config, registry, PRETTY_TAGS, Clock.SYSTEM) {
      @Override
      protected Double nullGaugeValue() {
        return null;
      }
    });
  }

  private static void registerAll(String prefix, MetricSet metricSet) {
    for (Map.Entry<String, Metric> entry : metricSet.getMetrics().entrySet()) {
      if (entry.getValue() instanceof MetricSet) {
        registerAll(prefix + "." + entry.getKey(), (MetricSet) entry.getValue());
      } else {
        registry.register(prefix + "." + entry.getKey(), entry.getValue());
      }
    }
  }

  private Metrics() {
    throw new AssertionError("not instantiable");
  }

  public static Meter meter(String name) {
    return registry.meter(PREFIX + name);
  }

  public static Timer timer(String name) {
    return registry.timer(PREFIX + name);
  }

  public static Gauge gauge(String name, MetricRegistry.MetricSupplier<Gauge> supplier) {
    // Some of our gauges are backed by connections to Couchbase or other server.
    // These must be recreated for each connection, so remove first.
    synchronized (Metrics.class) {
      // The metrics registry is a singleton, but the integration tests run many connectors in the
      // same process and they all compete to register gauges. Synchronize to prevent rare race condition
      // that can cause exception to be thrown here when running integration tests.
      registry.remove(PREFIX + name);
      return registry.gauge(PREFIX + name, supplier);
    }
  }

  public static MetricRegistry registry() {
    return registry;
  }

  private static final Meter bytesMeter = Metrics.meter("throughputBytes");
  private static final Meter rejectionMeter = Metrics.meter("docRejected"); // ES said "bad request"
  private static final Meter rejectionLogFailureMeter = Metrics.meter("rejectionLogFail");
  private static final Meter indexingRetryMeter = Metrics.meter("docWriteRetry");
  private static final Timer bulkIndexingTimer = Metrics.timer("bulkIndexPerDoc");
  private static final Timer retryDelayTimer = Metrics.timer("retryDelay");
  private static final Meter bulkRetriesMeter = Metrics.meter("bulkRetry");
  private static final Meter httpFailures = Metrics.meter("esConnFail");
  private static final Timer latencyTimer = Metrics.timer("latency");

  public static Meter bytesMeter() {
    return bytesMeter;
  }

  public static Meter rejectionLogFailureMeter() {
    return rejectionLogFailureMeter;
  }

  public static Meter rejectionMeter() {
    return rejectionMeter;
  }

  public static Meter indexingRetryMeter() {
    return indexingRetryMeter;
  }

  public static Timer indexTimePerDocument() {
    return bulkIndexingTimer;
  }

  public static Meter bulkRetriesMeter() {
    return bulkRetriesMeter;
  }

  public static Meter elasticsearchHostOffline() {
    return httpFailures;
  }

  public static Timer retryDelayTimer() {
    return retryDelayTimer;
  }

  public static Timer latencyTimer() {
    return latencyTimer;
  }

  private static final ObjectMapper mapper = new ObjectMapper();

  static {
    mapper.registerModule(new MetricsModule(SECONDS, MILLISECONDS, false));
  }

  public static String toJson() {
    return toJson(false);
  }

  public static String toJson(boolean pretty) {
    try {
      return (pretty ? mapper.writerWithDefaultPrettyPrinter() : mapper.writer())
          .writeValueAsString(toJsonNode());
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static JsonNode toJsonNode() {
    return mapper.convertValue(registry(), JsonNode.class);
  }
}
