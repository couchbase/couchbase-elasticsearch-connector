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
/*
import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.kv.PutResponse;

import java.util.concurrent.CompletableFuture;
*/
public class EtcdSandbox {
/*
  public static void main(String[] args) throws Exception {
    Client client = Client.builder().endpoints("http://localhost:2379").build();
    KV kvClient = client.getKVClient();

    ByteSequence key = ByteSequence.fromString("test_key");
    ByteSequence value = ByteSequence.fromString("test_value");

// put the key-value
    PutResponse putResponse = kvClient.put(key, value).get();


// get the CompletableFuture
    CompletableFuture<GetResponse> getFuture = kvClient.get(key);

// get the value from CompletableFutureebug
    GetResponse response = getFuture.get();

// delete the key
    kvClient.delete(key).get();

    client.close();
  }*/
}
