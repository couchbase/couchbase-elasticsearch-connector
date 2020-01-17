/*
 * Copyright 2020 Couchbase, Inc.
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
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.function.Supplier;

/**
 * A set of gauges for JVM process CPU usage and system CPU usage.
 * Values are percentages with one decimal point of precision.
 */
public class CpuGaugeSet implements MetricSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(CpuGaugeSet.class);

  private static final Supplier<OptionalDouble> processCpuLoad = cpuLoad("ProcessCpuLoad");
  private static final Supplier<OptionalDouble> systemCpuLoad = cpuLoad("SystemCpuLoad");

  @Override
  public Map<String, Metric> getMetrics() {
    final Map<String, Metric> metrics = new HashMap<>();
    metrics.put("process", (Gauge<Double>) () -> processCpuLoad.get().orElse(-1));
    metrics.put("system", (Gauge<Double>) () -> systemCpuLoad.get().orElse(-1));
    return Collections.unmodifiableMap(metrics);
  }

  private static Supplier<OptionalDouble> cpuLoad(String attrName) {
    // Access via the MBean server to avoid depending on
    // com.sun.management.OperatingSystemMXBean which
    // might not be present in the JDK/JVM distribution.
    //
    // Inspired by https://stackoverflow.com/a/21961088/611819
    //
    try {
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      ObjectName objectName = ObjectName.getInstance("java.lang:type=OperatingSystem");
      String[] attributes = {attrName};

      return () -> {
        try {
          AttributeList list = mBeanServer.getAttributes(objectName, attributes);

          if (list.isEmpty()) {
            return OptionalDouble.empty();
          }

          Attribute att = (Attribute) list.get(0);
          Double value = (Double) att.getValue();

          // usually takes a couple of seconds before we get real values
          if (value < 0) {
            return OptionalDouble.empty();
          }

          // returns a percentage value with 1 decimal point precision
          return OptionalDouble.of((int) (value * 1000) / 10.0);

        } catch (Exception e) {
          LOGGER.debug("Failed to read '{}' from MBean server", attrName, e);
          return OptionalDouble.empty();
        }
      };

    } catch (Exception e) {
      LOGGER.info("Failed to get MBean server; won't be able to report CPU usage metrics.", e);
      return OptionalDouble::empty;
    }
  }
}
