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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.json.MetricsModule;
import com.couchbase.client.dcp.metrics.DefaultDropwizardConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.dropwizard.DropwizardMeterRegistry;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Provides static access to a shared metrics registry.
 */
public class Metrics {
  private static final Logger log = LoggerFactory.getLogger(Metrics.class);

  public static final HierarchicalNameMapper PRETTY_TAGS = (id, convention) -> {
    String base = id.getConventionName(convention);

    if (base.startsWith("dcp")) {
      base = "dcp." + Character.toLowerCase(base.charAt(3)) + base.substring(4);
    } else if (base.startsWith("cbes")) {
      base = "cbes." + Character.toLowerCase(base.charAt(4)) + base.substring(5);
    } else if (base.startsWith("jvm")) {
      base = "jvm." + Character.toLowerCase(base.charAt(3)) + base.substring(4);
    }

    List<Tag> tags = id.getConventionTags(convention);
    if (tags.isEmpty()) {
      return base;
    }
    return base + "{" + tags.stream()
        .map(t -> t.getKey() + "=" + t.getValue())
        .map(nameSegment -> nameSegment.replace(" ", "_"))
        .collect(Collectors.joining(","))
        + "}";
  };

  private static final PrometheusMeterRegistry prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

  static {
    prometheusRegistry.config()
        .meterFilter(MeterFilter.denyNameStartsWith("cbes.backfill")) // deprecated, no reason to expose
        .meterFilter(MeterFilter.denyNameStartsWith("cbes.es.wait.ms")); // superseded by "cbes.es.wait.seconds"
  }

  private static final MetricRegistry dropwizardBackingRegistry = new MetricRegistry();
  private static final DropwizardMeterRegistry dropwizardRegistry = new DropwizardMeterRegistry(new DefaultDropwizardConfig(), dropwizardBackingRegistry, PRETTY_TAGS, Clock.SYSTEM) {
    @Override
    protected Double nullGaugeValue() {
      return null;
    }
  };

  private static final String PREFIX = "cbes.";

  private static final CompositeMeterRegistry registry = new CompositeMeterRegistry();

  public static CompositeMeterRegistry registry() {
    return registry;
  }

  static {
    registry.add(prometheusRegistry);
    registry.add(dropwizardRegistry);

    new ClassLoaderMetrics().bindTo(registry);
    new JvmMemoryMetrics().bindTo(registry);
    new JvmGcMetrics().bindTo(registry);
    new ProcessorMetrics().bindTo(registry);
    new JvmThreadMetrics().bindTo(registry);
  }

  private Metrics() {
    throw new AssertionError("not instantiable");
  }

  public static Counter counter(String name, String description) {
    return counter(name, description, null);
  }

  public static Counter counter(String name, String description, String unit) {
    return Counter.builder(PREFIX + name)
        .baseUnit(unit)
        .description(description)
        .register(registry);
  }

  public static Timer timer(String name, String description) {
    return Timer.builder(PREFIX + name)
        .description(description)
        .register(registry);
  }

  public static <T> T gauge(String name, String description, T stateObject, ToDoubleFunction<T> valueFunction) {
    // Some of our gauges are backed by connections to Couchbase or other server.
    // These must be recreated for each connection, so remove first.
    synchronized (Metrics.class) {
      // The metrics registry is a singleton, but the integration tests run many connectors in the
      // same process and they all compete to register gauges. Synchronize to prevent rare race condition
      // that can cause exception to be thrown here when running integration tests.
      registry.remove(new Meter.Id(PREFIX + name, Tags.empty(), null, description, Meter.Type.GAUGE));
      Gauge.builder(PREFIX + name, stateObject, valueFunction)
          .description(description)
          .register(registry);
      return stateObject;
    }
  }

  /**
   * For gauges whose values should be cached to prevent repeated calculation during reporting
   * when multiple reports are used.
   */
  public static <T> T cachedGauge(String name, String description, T stateObject, ToDoubleFunction<T> valueFunction, Duration expiration) {
    Supplier<Double> memoized = Suppliers.memoizeWithExpiration(() -> valueFunction.applyAsDouble(stateObject), expiration.toMillis(), MILLISECONDS);
    return gauge(name, description, stateObject, value -> memoized.get());
  }

  /**
   * For gauges whose values should be cached to prevent repeated calculation during reporting
   * when multiple reports are used. Uses default expiry duration.
   */
  public static <T> T cachedGauge(String name, String description, T stateObject, ToDoubleFunction<T> valueFunction) {
    final Duration DEFAULT_EXPIRY = Duration.ofSeconds(1);
    return cachedGauge(name, description, stateObject, valueFunction, DEFAULT_EXPIRY);
  }

  private static final String BYTES = "bytes";

  private static final Counter bytesMeter = Metrics.counter("throughput.bytes", "An estimate of the number of bytes the connector has written to Elasticsearch.", BYTES);
  private static final Counter rejectionCounter = Metrics.counter("doc.rejected", "Permanent indexing failure; usually result in an entry being added to the rejection log Elasticsearch index."); // ES said "bad request"
  private static final Counter rejectionLogFailureCounter = Metrics.counter("rejection.log.fail", "Failure to add a record to the rejection log Elasticsearch index.");
  private static final Counter indexingRetryCounter = Metrics.counter("doc.write.retry", "Failure to write an individual document. (For each `bulkRetry` event, one or more `docWriteRetry` events are recorded, indicating how many failures there were in the bulk request.)");
  private static final Timer bulkIndexingTimer = Metrics.timer("bulk.index.per.doc", "Duration of an Elasticsearch bulk request (including retries), divided by the number of items in the bulk request.");
  private static final Timer retryDelayTimer = Metrics.timer("retry.delay", "Time spent waiting after a temporary indexing failure before the request is retried.");
  private static final Counter bulkRetriesCounter = Metrics.counter("bulk.retry", "Elasticsearch bulk request retry due to a temporary failure.");
  private static final Counter httpFailures = Metrics.counter("es.conn.fail", "Failed Elasticsearch connection attempts.");
  private static final Timer latencyTimer = Metrics.timer("latency", "The time between when the connector is notified of a database change and when the change is written to Elasticsearch.");

  public static Counter bytesCounter() {
    return bytesMeter;
  }

  public static Counter rejectionLogFailureCounter() {
    return rejectionLogFailureCounter;
  }

  public static Counter rejectionCounter() {
    return rejectionCounter;
  }

  public static Counter indexingRetryCounter() {
    return indexingRetryCounter;
  }

  public static Timer indexTimePerDocument() {
    return bulkIndexingTimer;
  }

  public static Counter bulkRetriesCounter() {
    return bulkRetriesCounter;
  }

  public static Counter elasticsearchHostOffline() {
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
    Stopwatch timer = Stopwatch.createStarted();
    try {
      return (pretty ? mapper.writerWithDefaultPrettyPrinter() : mapper.writer())
          .writeValueAsString(toJsonNode());
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    } finally {
      log.info("Serializing metrics as JSON took " + timer);
    }
  }

  public static JsonNode toJsonNode() {
    return mapper.convertValue(dropwizardBackingRegistry, JsonNode.class);
  }

  public static String toPrometheusExpositionFormat() {
    return prometheusRegistry.scrape();
  }

  public static MetricRegistry dropwizardRegistry() {
    return dropwizardBackingRegistry;
  }
}
