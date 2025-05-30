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

import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.testcontainers.CustomCouchbaseContainer;
import com.github.therapi.core.internal.LangHelper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.StringUtils.substringAfterLast;
import static org.apache.commons.lang3.StringUtils.substringBeforeLast;

public class TestConfigHelper {

  public static String readConfig() throws IOException {
    return Files.asCharSource(new File("src/dist/config/example-connector.toml"), UTF_8).read();
  }

  public static String readConfig(CustomCouchbaseContainer couchbase, SinkContainer elasticsearch) throws IOException {
    return readConfig(couchbase, elasticsearch, emptyMap());
  }

  public static String readConfig(CustomCouchbaseContainer couchbase, SinkContainer elasticsearch,
                                  Map<String, Object> propertyOverrides) throws IOException {


    final Map<String, Object> defaultOverrides = ImmutableMap.of(
        "metrics.httpPort", -1,
        "metrics.logInterval", "0",
        "elasticsearch.hosts", singletonList(elasticsearch.getHttpHostAddress()),
        "couchbase.hosts", singletonList(couchbase.getHost() + ":" + couchbase.getMappedPort(11210) + "=kv")
//        ,"couchbase.network", "external"
    );

    final Map<String, Object> effectiveOverrides = new HashMap<>(defaultOverrides);
    effectiveOverrides.putAll(propertyOverrides);

    checkState(elasticsearch.isRunning()); // Can only get CA cert after container starts
    elasticsearch.caCertAsBytes().ifPresent(certBytes -> {
      try {
        File caFile = new File("build/tmp/elasticsearch-ca-cert.pem");
        Files.createParentDirs(caFile);
        Files.write(certBytes, caFile);

        effectiveOverrides.put("elasticsearch.secureConnection", true);
        effectiveOverrides.put("elasticsearch.pathToCaCertificate", caFile.getAbsolutePath());

        System.out.println("Copied Elasticsearch CA certificate to file: " + caFile.getAbsolutePath());

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    return readConfig(effectiveOverrides);
  }

  private static String readConfig(Map<String, Object> propertyOverrides) throws IOException {
    String config = readConfig();
    for (Map.Entry<String, Object> override : propertyOverrides.entrySet()) {
      config = patchToml(config, override.getKey(), override.getValue());
    }

    // validate
    ConnectorConfig.from(config);

    return config;
  }

  private static String patchToml(String input, String key, Object newValue) {
    final String table = substringBeforeLast(key, ".");
    final String field = substringAfterLast(key, ".");

    final Pattern pattern = Pattern.compile(
        "(^\\s*" + Pattern.quote("[" + table + "]") + ".*?" +
            "^\\s*" + Pattern.quote(field) + "\\s*=\\s*).+?$", Pattern.MULTILINE | Pattern.DOTALL);
    final String result = LangHelper.replace(input, pattern, m -> m.group(1) + Matcher.quoteReplacement(formatTomlValue(newValue)));
    if (input.equals(result)) {
      throw new IllegalArgumentException("no match for key: " + key);
    }
    return result;
  }

  private static String formatTomlValue(Object o) {
    if (o instanceof String) {
      String s = (String) o;
      return '"' + s.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
    }

    if (o instanceof Iterable) {
      return "[" + StreamSupport.stream(((Iterable<?>) o).spliterator(), false)
          .map(TestConfigHelper::formatTomlValue)
          .collect(Collectors.joining(", ")) + "]";
    }

    return o.toString();
  }



}
