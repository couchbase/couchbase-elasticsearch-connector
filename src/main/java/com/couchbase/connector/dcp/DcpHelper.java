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

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.DefaultConnectionNameGenerator;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.RollbackMessage;
import com.couchbase.client.dcp.state.FailoverLogEntry;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.IllegalReferenceCountException;
import com.couchbase.connector.VersionHelper;
import com.couchbase.connector.cluster.Coordinator;
import com.couchbase.connector.config.common.CouchbaseConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.CompletableSubscriber;
import rx.Subscription;

import java.io.IOException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

public class DcpHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(DcpHelper.class);

  private DcpHelper() {
    throw new AssertionError("not instantiable");
  }

  public static String metadataDocumentIdPrefix() {
    return "_connector:cbes:";
  }

  public static boolean isMetadata(Event e) {
    return e.getKey().startsWith(metadataDocumentIdPrefix());
  }

  public static void ackAndRelease(ChannelFlowController flowController, ByteBuf buffer) throws IllegalReferenceCountException {
    try {
      flowController.ack(buffer);

    } catch (IllegalReferenceCountException e) {
      throw e;

    } catch (Exception e) {
      LOGGER.warn("Flow control ack failed (channel already closed?)", e);
    }

    buffer.release();
  }

  public static Client newClient(String groupName, CouchbaseConfig config, Supplier<KeyStore> keystore) {
    final Client.Builder builder = Client.configure()
        .connectionNameGenerator(DefaultConnectionNameGenerator.forProduct("elasticsearch-connector", VersionHelper.getVersion(), groupName))
        .connectTimeout(config.dcp().connectTimeout().millis())
        .hostnames(config.hosts())
        .networkResolution(config.network())
        .bucket(config.bucket())
//          .poolBuffers(true)
        .username(config.username())
        .password(config.password())
        .controlParam(DcpControl.Names.ENABLE_NOOP, "true")
        .controlParam(DcpControl.Names.SET_NOOP_INTERVAL, 20)
        .compression(config.dcp().compression())
        .mitigateRollbacks(
            config.dcp().persistencePollingInterval().duration(),
            config.dcp().persistencePollingInterval().timeUnit())
        .flowControl(toIntOrDie(config.dcp().flowControlBuffer().getBytes()))
        .bufferAckWatermark(60);

    if (config.secureConnection()) {
      builder.sslEnabled(true);
      builder.sslKeystore(keystore.get());
    }

    return builder.build();
  }

  private static int toIntOrDie(long l) {
    if (l > Integer.MAX_VALUE || l < Integer.MIN_VALUE) {
      throw new IllegalArgumentException("Magnitude of value " + l + " is too large to be represented by int");
    }
    return (int) l;
  }

  /**
   * WARNING: Messes with the session state, so call it *before* opening streams
   * or initializing the session to your desired state.
   */
  public static ImmutableList<Long> getCurrentSeqnos(Client dcpClient, Set<Integer> partitions) {
    final int numPartitions = dcpClient.numPartitions();

    dcpClient.initializeState(StreamFrom.NOW, StreamTo.INFINITY).await();
    final Long[] backfillTargetSeqno = new Long[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      backfillTargetSeqno[i] = partitions.contains(i) ? dcpClient.sessionState().get(i).getStartSeqno() : 0;
    }

    // !! Leave the session state the way it is, so CheckpointClear can inspect it

    return ImmutableList.copyOf(backfillTargetSeqno);
  }

  /**
   * @param eventSink responsible for processing the event (usually asynchronously)
   * and calling {@link Event#release()} when finished
   */
  public static void initDataEventHandler(Client dcpClient, Consumer<Event> eventSink, SnapshotMarker[] snapshots) {
    dcpClient.dataEventHandler((flowController, event) -> {
      if (DcpMutationMessage.is(event) || DcpDeletionMessage.is(event) || DcpExpirationMessage.is(event)) {
        final short vbucket = MessageUtil.getVbucket(event);
        final long vbuuid = dcpClient.sessionState().get(vbucket).getLastUuid();

        final Event e = new Event(event, flowController, vbuuid, snapshots[vbucket]);
        //LOGGER.trace("GOT DATA EVENT: {}", e);
        eventSink.accept(e);

      } else {
        LOGGER.warn("Unexpected data event type '{}'", event.readableBytes() > 0 ? event.getByte(1) : "<zero length>");
        ackAndRelease(flowController, event);
      }
    });
  }

  public static void initControlHandler(Client dcpClient, Coordinator coordinator, SnapshotMarker[] snapshots) {
    dcpClient.controlEventHandler((flowController, event) -> {
      try {
        if (DcpSnapshotMarkerRequest.is(event)) {
          final int vbucket = DcpSnapshotMarkerRequest.partition(event);
          final SnapshotMarker snapshot = new SnapshotMarker(
              DcpSnapshotMarkerRequest.startSeqno(event),
              DcpSnapshotMarkerRequest.endSeqno(event));
          snapshots[vbucket] = snapshot;
          LOGGER.debug("Snapshot for partition {}: {}", vbucket, snapshot);

        } else if (RollbackMessage.is(event)) {
          final short partition = RollbackMessage.vbucket(event);
          final long seqno = RollbackMessage.seqno(event);

          LOGGER.warn("Rolling back partition {} to seqno {}", partition, seqno);

          // Careful, we're in the Netty IO thread, so must not await completion.
          dcpClient.rollbackAndRestartStream(partition, seqno)
              .subscribe(new CompletableSubscriber() {
                @Override
                public void onCompleted() {
                  LOGGER.info("Rollback for partition {} complete", partition);
                }

                @Override
                public void onError(Throwable e) {
                  coordinator.panic("Failed to roll back partition " + partition + " to seqno " + seqno, e);
                }

                @Override
                public void onSubscribe(Subscription d) {
                }
              });
        }
      } finally {
        ackAndRelease(flowController, event);
      }
    });
  }

  public static void initSessionState(Client dcpClient, CheckpointService checkpointService, Set<Integer> partitions) throws IOException {
    final Map<Integer, Checkpoint> positions = checkpointService.load(partitions);

    final SessionState sessionState = dcpClient.sessionState();

    LOGGER.debug("Initializing DCP session state from checkpoint: {}", positions);

    for (Map.Entry<Integer, Checkpoint> entry : positions.entrySet()) {
      final int partition = entry.getKey();
      final Checkpoint checkpoint = entry.getValue();
      if (checkpoint == null) {
        continue;
      }

      final PartitionState ps = sessionState.get(partition);
      ps.setStartSeqno(checkpoint.getSeqno());
      ps.setSnapshotStartSeqno(checkpoint.getSnapshot().getStartSeqno());
      ps.setSnapshotEndSeqno(checkpoint.getSnapshot().getEndSeqno());

      // Use seqno -1 (max unsigned) so this synthetic failover log entry will always be pruned
      // if the initial streamOpen request gets a rollback response. If there's no rollback
      // on initial request, then the seqno used here doesn't matter, because the failover log
      // gets reset when the stream is opened.
      ps.setFailoverLog(singletonList(new FailoverLogEntry(-1L, checkpoint.getVbuuid())));
      LOGGER.debug("Initialized partition {} state = {}", partition, ps);
    }
  }

  public static List<Integer> allPartitions(Client dcpClient) {
    return allPartitions(dcpClient.numPartitions());
  }

  public static List<Integer> allPartitions(int numPartitions) {
    final List<Integer> allPartitions = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      allPartitions.add(i);
    }
    return allPartitions;
  }

  public static Short[] toBoxedShortArray(Iterable<? extends Number> numbers) {
    final List<Short> result = new ArrayList<>();
    for (Number i : numbers) {
      result.add(i.shortValue());
    }
    return Iterables.toArray(result, Short.class);
  }
}
