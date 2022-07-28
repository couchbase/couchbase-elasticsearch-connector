/*
 * Copyright 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connector.config;

import org.junit.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static com.couchbase.connector.config.ConfigHelper.createHttpHost;
import static com.couchbase.connector.config.ConfigHelper.parseTime;
import static org.junit.Assert.assertEquals;

public class ConfigHelperTest {

  @Test
  public void canCreateHttpHost() throws Exception {
    assertEquals("http://localhost:9200", createHttpHost("localhost", 9200, false).toURI());
    assertEquals("http://localhost:9201", createHttpHost("localhost:9201", 9200, false).toURI());
    assertEquals("https://localhost:9200", createHttpHost("localhost", 9200, true).toURI());
    assertEquals("https://localhost:9201", createHttpHost("localhost:9201", 9200, true).toURI());

    assertEquals("http://localhost:9200", createHttpHost("http://localhost", 9200, false).toURI());
    assertEquals("http://localhost:9201", createHttpHost("http://localhost:9201", 9200, false).toURI());
    assertEquals("https://localhost:9200", createHttpHost("https://localhost", 9200, true).toURI());
    assertEquals("https://localhost:9201", createHttpHost("https://localhost:9201", 9200, true).toURI());
  }

  @Test(expected = ConfigException.class)
  public void insecureHttpsDefaultPort() throws Exception {
    createHttpHost("https://localhost", 9200, false);
  }

  @Test(expected = ConfigException.class)
  public void insecureHttpsCustomPort() throws Exception {
    createHttpHost("https://localhost:1234", 9200, false);
  }

  @Test(expected = ConfigException.class)
  public void secureHttpCustomPort() throws Exception {
    createHttpHost("http://localhost:1234", 9200, true);
  }

  @Test(expected = ConfigException.class)
  public void secureHttpDefaultPort() throws Exception {
    createHttpHost("http://localhost", 9200, true);
  }

  @Test
  public void canParseElasticsearchDurations() {
    assertEquals(Duration.ofNanos(10), parseTime("10ns"));
    assertEquals(Duration.ofNanos(10), parseTime("10nanos"));
    assertEquals(Duration.ofNanos(10), parseTime(" 10  nanos "));
    assertEquals(Duration.of(10, ChronoUnit.MICROS), parseTime(" 10  micros "));

    assertEquals(Duration.ofNanos(10), parseTime("10NANOS"));
    assertEquals(Duration.ofHours(10), parseTime("10H"));
  }
}
