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

package com.couchbase.connector.dcp;


import com.couchbase.connector.elasticsearch.BucketMismatchException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryCheckpointDao implements CheckpointDao {
  private final Map<Integer, Checkpoint> store = new ConcurrentHashMap<>();
  private volatile String bucketUuid;

  @Override
  public void save(String bucketUuid, Map<Integer, Checkpoint> vbucketToCheckpoint) throws IOException {
    for (Map.Entry<Integer, Checkpoint> entry : vbucketToCheckpoint.entrySet()) {
      if (entry.getValue() != null) {
        store.put(entry.getKey(), entry.getValue());
      }
    }
    this.bucketUuid = bucketUuid;
  }

  public Map<Integer, Checkpoint> load(String bucketUuid, Set<Integer> vbuckets) throws IOException {
    if (!bucketUuid.equals(this.bucketUuid)) {
      throw new BucketMismatchException();
    }

    final Map<Integer, Checkpoint> result = new HashMap<>(store);
    result.keySet().retainAll(vbuckets);
    return result;
  }

  @Override
  public void clear(String bucketUuid, Set<Integer> vbuckets) {
    store.keySet().removeAll(vbuckets);
  }

  @Override
  public String toString() {
    return store.toString();
  }
}
