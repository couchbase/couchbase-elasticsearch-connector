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
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.config.HostAndPort;
import com.couchbase.client.dcp.core.env.NetworkResolution;
import com.couchbase.client.dcp.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.dcp.deps.io.netty.util.IllegalReferenceCountException;
import com.couchbase.client.dcp.highlevel.DatabaseChangeListener;
import com.couchbase.client.dcp.highlevel.Deletion;
import com.couchbase.client.dcp.highlevel.DocumentChange;
import com.couchbase.client.dcp.highlevel.Mutation;
import com.couchbase.client.dcp.highlevel.SnapshotMarker;
import com.couchbase.client.dcp.highlevel.StreamFailure;
import com.couchbase.client.dcp.state.FailoverLogEntry;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.connector.VersionHelper;
import com.couchbase.connector.cluster.Coordinator;
import com.couchbase.connector.config.common.CouchbaseConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;

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

  public static Client newClient(String groupName, CouchbaseConfig config, ResolvedBucketConfig bucketConfig, Supplier<KeyStore> keystore) {

    // ES connector bootstraps using Manager port, but the DCP client wants KV port.
    // Get the KV ports from the bucket config!
    Set<String> seedNodes = bucketConfig.getKvAddresses().stream()
        .map(HostAndPort::format)
        .collect(toSet());

    final Client.Builder builder = Client.builder()
        .userAgent("elasticsearch-connector", VersionHelper.getVersion(), groupName)
        .connectTimeout(config.dcp().connectTimeout().millis())
        .seedNodes(seedNodes)
        .networkResolution(NetworkResolution.valueOf(config.network().name()))
        .bucket(config.bucket())
//          .poolBuffers(true)
        .credentials(config.username(), config.password())
        .controlParam(DcpControl.Names.SET_NOOP_INTERVAL, 20)
        .compression(config.dcp().compression())
        .mitigateRollbacks(
            config.dcp().persistencePollingInterval().duration(),
            config.dcp().persistencePollingInterval().timeUnit())
        .flowControl(toSaturatedInt(config.dcp().flowControlBuffer().getBytes()))
        .bufferAckWatermark(60);

    if (config.secureConnection()) {
      builder.sslEnabled(true);
      builder.sslKeystore(keystore.get());
    }

    return builder.build();
  }

  private static int toSaturatedInt(long value) {
    return (int) Math.min(Integer.MAX_VALUE, Math.max(Integer.MIN_VALUE, value));
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

  public static void initEventListener(Client dcpClient, Coordinator coordinator, Consumer<Event> eventSink) {
    dcpClient.nonBlockingListener(new DatabaseChangeListener() {
      @Override
      public void onFailure(StreamFailure streamFailure) {
        coordinator.panic("DCP stream failure.", streamFailure.getCause());
      }

      @Override
      public void onMutation(Mutation mutation) {
        onMutationOrDeletion(mutation);
      }

      @Override
      public void onDeletion(Deletion deletion) {
        onMutationOrDeletion(deletion);
      }

      private void onMutationOrDeletion(DocumentChange change) {
        eventSink.accept(new Event(change));
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
      ps.setSnapshot(
          new SnapshotMarker(
              checkpoint.getSnapshot().getStartSeqno(),
              checkpoint.getSnapshot().getEndSeqno()));

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
