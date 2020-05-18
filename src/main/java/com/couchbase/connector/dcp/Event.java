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

import com.couchbase.client.core.logging.RedactableArgument;
import com.couchbase.client.dcp.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.dcp.deps.io.netty.util.IllegalReferenceCountException;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;

import static com.couchbase.connector.dcp.DcpHelper.ackAndRelease;
import static java.util.Objects.requireNonNull;

public class Event {
  private final String key;
  private final ByteBuf byteBuf;
  private final ChannelFlowController flowController;
  private final long vbuuid;
  private final long seqno;
  private final SnapshotMarker snapshot;
  private final int vbucket;
  private final boolean mutation;
  private final long receivedNanos = System.nanoTime();
  private volatile byte[] content;

  public Event(ByteBuf byteBuf, ChannelFlowController flowController, long vbuuid, SnapshotMarker snapshot) {
    this.key = MessageUtil.getKeyAsString(byteBuf);
    this.byteBuf = byteBuf;
    this.flowController = flowController;
    this.vbuuid = vbuuid;
    this.vbucket = MessageUtil.getVbucket(byteBuf);
    this.seqno = DcpMutationMessage.bySeqno(byteBuf); // works for deletion and expiration, too
    this.mutation = DcpMutationMessage.is(byteBuf);
    this.snapshot = requireNonNull(snapshot, "null snapshot");
  }

  /**
   * Must be called when the connector is finished processing the event.
   *
   * @throws IllegalReferenceCountException if buffer has already been released
   */
  public void release() {
    ackAndRelease(flowController, byteBuf);
  }

  public int getVbucket() {
    return vbucket;
  }

  public long getVbuuid() {
    return vbuuid;
  }

  public String getKey() {
    return key;
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
    return seqno;
  }

  public Checkpoint getCheckpoint() {
    return new Checkpoint(getVbuuid(), getSeqno(), snapshot);
  }

  public ByteBuf getByteBuf() {
    return byteBuf;
  }

  public byte[] getContent() {
    if (content == null) {
      content = MessageUtil.getContentAsByteArray(byteBuf);
    }
    return content;
  }

  @Override
  public String toString() {
    final String type = isMutation() ? "MUT" : "DEL";
    return type + ":" + getVbucket() + "/" + getCheckpoint() + "=" + RedactableArgument.user(getKey());
  }
}
