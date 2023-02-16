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

package com.couchbase.connector.dcp;

import com.couchbase.client.core.util.ConnectionString;
import org.junit.Test;

import java.lang.reflect.Method;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static org.junit.Assert.assertEquals;

public class CouchbaseHelperTest {
  @Test
  public void getAlias() throws Exception {
    Method fromString = ConnectionString.PortType.class.getDeclaredMethod("fromString", String.class);
    fromString.setAccessible(true);

    for (ConnectionString.PortType type : ConnectionString.PortType.values()) {
      if (type == ConnectionString.PortType.PROTOSTELLAR) {
        continue;
      }
      String alias = CouchbaseHelper.getAlias(type);
      ConnectionString.PortType roundTrip = (ConnectionString.PortType) fromString.invoke(null, alias);
      assertEquals(type, roundTrip);
    }
  }

  @Test
  public void qualifyPorts() throws Exception {
    assertEquals(listOf("foo:123=manager", "bar", "zot:123=kv", "moo:123=manager"),
        CouchbaseHelper.qualifyPorts(
            listOf("foo:123", "bar", "zot:123=kv", "moo:123=manager"),
            ConnectionString.PortType.MANAGER));

    assertEquals(listOf("foo:123=kv", "bar", "zot:123=kv", "moo:123=manager"),
        CouchbaseHelper.qualifyPorts(
            listOf("foo:123", "bar", "zot:123=kv", "moo:123=manager"),
            ConnectionString.PortType.KV));
  }
}
