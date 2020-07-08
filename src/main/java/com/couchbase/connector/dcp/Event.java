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

import com.couchbase.client.dcp.highlevel.DocumentChange;
import com.couchbase.client.dcp.highlevel.Mutation;
import com.couchbase.client.dcp.highlevel.StreamOffset;

import static java.util.Objects.requireNonNull;

public class Event {
  private final DocumentChange change;
  private final boolean mutation;
  private final long receivedNanos = System.nanoTime();

  public Event(DocumentChange change) {
    this.change = requireNonNull(change);
    this.mutation = change instanceof Mutation;
  }

  /**
   * Must be called when the connector is finished processing the event.
   */
  public void release() {
    change.flowControlAck();
  }

  public int getVbucket() {
    return change.getVbucket();
  }

  public String getKey() {
    return change.getKey();
  }

  public String getKey(boolean qualifiedWithScopeAndCollection) {
    return qualifiedWithScopeAndCollection ? change.getQualifiedKey() : change.getKey();
  }

  public boolean isMutation() {
    return mutation;
  }

  public long getReceivedNanos() {
    return receivedNanos;
  }

  /**
   * Be aware that sequence numbers are unsigned, and must be compared using
   * {@link Long#compareUnsigned(long, long)}
   */
  public long getSeqno() {
    return change.getOffset().getSeqno();
  }

  public Checkpoint getCheckpoint() {
    return toCheckpoint(change.getOffset());
  }

  private static Checkpoint toCheckpoint(StreamOffset offset) {
    return new Checkpoint(offset.getVbuuid(), offset.getSeqno(),
        new SnapshotMarker(
            offset.getSnapshot().getStartSeqno(),
            offset.getSnapshot().getEndSeqno()));
  }

  public DocumentChange getChange() {
    return change;
  }

  public byte[] getContent() {
    return change.getContent();
  }

  @Override
  public String toString() {
    return change.toString();
  }
}
