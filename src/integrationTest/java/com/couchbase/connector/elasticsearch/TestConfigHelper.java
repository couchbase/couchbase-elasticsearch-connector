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

import com.couchbase.connector.config.common.ImmutableCouchbaseConfig;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.config.es.ImmutableConnectorConfig;
import com.couchbase.connector.testcontainers.CustomCouchbaseContainer;
import com.couchbase.connector.testcontainers.ElasticsearchContainer;
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.StringUtils.substringAfterLast;
import static org.apache.commons.lang3.StringUtils.substringBeforeLast;

public class TestConfigHelper {

  public static String readConfig() throws IOException {
    return Files.asCharSource(new File("src/dist/config/example-connector.toml"), UTF_8).read()
        // Rewrite the document type config for compatibility with ES 5.x.
        // (if you change this, also change code in TestEsClient that assumes type name is "doc")
        .replace("'_doc'", "'doc'");
  }

  public static String readConfig(CustomCouchbaseContainer couchbase, ElasticsearchContainer elasticsearch) throws IOException {
    return readConfig(couchbase, elasticsearch, emptyMap());
  }

  public static String readConfig(CustomCouchbaseContainer couchbase, ElasticsearchContainer elasticsearch,
                                  Map<String, Object> propertyOverrides) throws IOException {

    final String dockerHost = DockerHelper.getDockerHost();

    final Map<String, Object> defaultOverrides = ImmutableMap.of(
        "metrics.httpPort", -1,
        "metrics.logInterval", "0",
        "elasticsearch.hosts", singletonList(dockerHost + ":" + elasticsearch.getHost().getPort()),
        "couchbase.hosts", singletonList(dockerHost + ":" + couchbase.getMappedPort(8091))
    );

    final Map<String, Object> effectiveOverrides = new HashMap<>(defaultOverrides);
    effectiveOverrides.putAll(propertyOverrides);

    return readConfig(effectiveOverrides);
  }

  public static String readConfig(Map<String, Object> propertyOverrides) throws IOException {
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

  static ImmutableConnectorConfig withBucketName(ImmutableConnectorConfig config, String bucketName) {
    return config.withCouchbase(
        ImmutableCouchbaseConfig.copyOf(config.couchbase())
            .withBucket(bucketName));
  }
}
