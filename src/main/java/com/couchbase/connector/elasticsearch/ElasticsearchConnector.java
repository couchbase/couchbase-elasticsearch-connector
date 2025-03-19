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

import com.codahale.metrics.Slf4jReporter;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.logging.LogRedaction;
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.metrics.LogLevel;
import com.couchbase.client.dcp.util.PartitionSet;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.connector.cluster.DefaultPanicButton;
import com.couchbase.connector.cluster.Membership;
import com.couchbase.connector.cluster.PanicButton;
import com.couchbase.connector.cluster.k8s.ReplicaChangeWatcher;
import com.couchbase.connector.cluster.k8s.StatefulSetInfo;
import com.couchbase.connector.config.common.CouchbaseConfig;
import com.couchbase.connector.config.common.ImmutableGroupConfig;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.config.es.ImmutableConnectorConfig;
import com.couchbase.connector.dcp.Checkpoint;
import com.couchbase.connector.dcp.CheckpointDao;
import com.couchbase.connector.dcp.CheckpointService;
import com.couchbase.connector.dcp.CouchbaseCheckpointDao;
import com.couchbase.connector.dcp.CouchbaseHelper;
import com.couchbase.connector.dcp.DcpHelper;
import com.couchbase.connector.elasticsearch.cli.AbstractCliCommand;
import com.couchbase.connector.elasticsearch.cli.CheckpointClear;
import com.couchbase.connector.elasticsearch.io.RequestFactory;
import com.couchbase.connector.elasticsearch.sink.SinkOps;
import com.couchbase.connector.elasticsearch.sink.SinkWorkerGroup;
import com.couchbase.connector.util.HttpServer;
import com.couchbase.connector.util.KeyStoreHelper;
import com.couchbase.connector.util.RuntimeHelper;
import com.couchbase.connector.util.ThrowableHelper;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.connector.VersionHelper.getVersionString;
import static com.couchbase.connector.dcp.DcpHelper.initEventListener;
import static com.couchbase.connector.dcp.DcpHelper.initSessionState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ElasticsearchConnector extends AbstractCliCommand {

//  static {
//    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
//  }
//

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchConnector.class);

  private static class OptionsParser extends CommonParser {
  }

  private static Slf4jReporter newSlf4jReporter(Duration logInterval) {
    Slf4jReporter reporter = Slf4jReporter.forRegistry(Metrics.dropwizardRegistry())
        .convertDurationsTo(MILLISECONDS)
        .convertRatesTo(SECONDS)
        .outputTo(LoggerFactory.getLogger("cbes.metrics"))
        .withLoggingLevel(Slf4jReporter.LoggingLevel.INFO)
        .build();
    if (logInterval.toMillis() > 0) {
      reporter.start(logInterval.toMillis(), logInterval.toMillis(), MILLISECONDS);
    }
    return reporter;
  }

  private static ConnectorConfig transformMembership(ConnectorConfig config, Function<Membership, Membership> transformer) {
    return ImmutableConnectorConfig.copyOf(config)
        .withGroup(ImmutableGroupConfig.copyOf(config.group())
            .withStaticMembership(transformer.apply(config.group().staticMembership())));
  }

  public static void main(String... args) throws Throwable {
    LOGGER.info("Couchbase Elasticsearch Connector version {}", getVersionString());

    final OptionsParser parser = new OptionsParser();
    final OptionSet options = parser.parse(args);

    final File configFile = options.valueOf(parser.configFile);
    System.out.println("Reading connector configuration from " + configFile.getAbsoluteFile());
    ConnectorConfig config = ConnectorConfig.from(configFile);

    if (config.trustStore().isPresent()) {
      LOGGER.warn("The [truststore] config section is DEPRECATED and will be removed in a future release." +
          " Please specify the Couchbase and/or Elasticsearch CA certificates by setting `pathToCaCertificate`" +
          " in the [couchbase] and/or [elasticsearch] config sections. The files identified by `pathToCaCertificate`" +
          " should contain one or more certificates in PEM format.");
    }

    final PanicButton panicButton = new DefaultPanicButton();

    boolean watchK8sReplicas = "true".equals(System.getenv("CBES_K8S_WATCH_REPLICAS"));
    boolean getMemberNumberFromHostname = watchK8sReplicas || "true".equals(System.getenv("CBES_K8S_STATEFUL_SET"));

    if (getMemberNumberFromHostname) {
      int memberNumber = StatefulSetInfo.fromHostname().podOrdinal + 1;
      LOGGER.info("Getting group member number from Kubernetes pod hostname: {}", memberNumber);

      // This is a kludge. The Membership class validates its arguments, so you can't have a Membership
      // of "4 of 1", for example. If we plan to get the group size from the Kubernetes StatefulSet,
      // bypass this validation by temporarily setting the group size to the largest sane value (1024).
      // We'll dial it down to the actual size of the StatefulSet a bit later on.
      int clusterSize = watchK8sReplicas ? 1024 : config.group().staticMembership().getClusterSize();
      config = transformMembership(config, m -> Membership.of(memberNumber, clusterSize));
    }

    KubernetesClient k8sClient = null;
    try {
      if (watchK8sReplicas) {
        k8sClient = new DefaultKubernetesClient();

        LOGGER.info("Activating native Kubernetes integration; connector will use StatefulSet spec" +
            " to determine group size." +
            " This mode requires a Kubernetes service account with 'get' and 'watch', and 'list'" +
            " permissions for the StatefulSet.");

        int k8sReplicas = ReplicaChangeWatcher.getReplicasAndPanicOnChange(k8sClient, panicButton);

        config = transformMembership(config, m -> Membership.of(m.getMemberNumber(), k8sReplicas));
      }

      if (watchK8sReplicas || getMemberNumberFromHostname) {
        LOGGER.info("Patched configuration with info from Kubernetes environment; membership = {}",
            config.group().staticMembership());
      }

      if (config.group().staticMembership().getClusterSize() > 1024) {
        panicButton.panic("Invalid group size configuration; totalMembers must be <= 1024." +
            " Did you forget to set the CBES_TOTAL_MEMBERS environment variable?");
      }

      Duration startupQuietPeriod = watchK8sReplicas ? ReplicaChangeWatcher.startupQuietPeriod() : Duration.ZERO;

      run(config, panicButton, startupQuietPeriod);
    } finally {
      if (k8sClient != null) {
        k8sClient.close(); // so client threads don't prevent app from exiting
      }
    }
  }

  public static class ConnectorRunner {
    private final ConnectorConfig config;
    private PanicButton panicButton = new DefaultPanicButton();
    private Duration startupQuietPeriod = Duration.ZERO;
    private Runnable onReady = () -> {
    };

    public ConnectorRunner(ConnectorConfig config) {
      this.config = config;
    }

    public ConnectorRunner panicButton(PanicButton panicButton) {
      this.panicButton = panicButton;
      return this;
    }

    public ConnectorRunner onReady(Runnable onReady) {
      this.onReady = onReady;
      return this;
    }

    public ConnectorRunner startupQuietPeriod(Duration startupQuietPeriod) {
      this.startupQuietPeriod = startupQuietPeriod;
      return this;
    }

    public void run() throws Throwable {
      ElasticsearchConnector.run(config, panicButton, startupQuietPeriod, onReady);
    }
  }

  public static void run(ConnectorConfig config) throws Throwable {
    new ConnectorRunner(config).run();
  }

  public static void run(ConnectorConfig config, PanicButton panicButton, Duration startupQuietPeriod) throws Throwable {
    new ConnectorRunner(config)
        .panicButton(panicButton)
        .startupQuietPeriod(startupQuietPeriod)
        .run();
  }

  private static void run(ConnectorConfig config, PanicButton panicButton, Duration startupQuietPeriod, Runnable onReady) throws Throwable {
    final Throwable fatalError;

    final Membership membership = config.group().staticMembership();

    LOGGER.info("Read configuration: {}", redactSystem(config));
    LOGGER.info("Couchbase CA certificate(s): {}", KeyStoreHelper.describe(config.couchbase().caCert()));
    LOGGER.info("Elasticsearch CA certificate(s): {}", KeyStoreHelper.describe(config.elasticsearch().caCert()));

    final ScheduledExecutorService checkpointExecutor = Executors.newSingleThreadScheduledExecutor();

    DocumentLifecycle.setLogLevel(config.logging().logDocumentLifecycle() ? LogLevel.INFO : LogLevel.DEBUG);
    LogRedaction.setRedactionLevel(config.logging().redactionLevel());
    DcpHelper.setRedactionLevel(config.logging().redactionLevel());

    try (Slf4jReporter metricReporter = newSlf4jReporter(config.metrics().logInterval());
         HttpServer httpServer = new HttpServer(config.metrics().httpPort(), membership);
         SinkOps esClient = SinkOps.create(config)
    ) {
      final Cluster cluster = CouchbaseHelper.createCluster(config);

      // Wait for couchbase server to come online, then open the bucket.
      final Bucket bucket = CouchbaseHelper.waitForBucket(cluster, config.couchbase().bucket());
      final Set<SeedNode> kvNodes = CouchbaseHelper.getKvNodes(config.couchbase(), bucket);

      final boolean storeMetadataInSourceBucket = config.couchbase().metadataBucket().equals(config.couchbase().bucket());
      final Bucket metadataBucket = storeMetadataInSourceBucket ? bucket : CouchbaseHelper.waitForBucket(cluster, config.couchbase().metadataBucket());
      final Collection metadataCollection = CouchbaseHelper.getMetadataCollection(metadataBucket, config.couchbase());

      final CheckpointDao checkpointDao = new CouchbaseCheckpointDao(metadataCollection, config.group().name());

      final String bucketUuid = ""; // todo get this from dcp client
      final CheckpointService checkpointService = new CheckpointService(bucketUuid, checkpointDao);
      final RequestFactory requestFactory = new RequestFactory(
          config.elasticsearch().types(), config.elasticsearch().docStructure(), config.elasticsearch().rejectLog());

      final SinkWorkerGroup workers = new SinkWorkerGroup(
          esClient,
          checkpointService,
          requestFactory,
          ErrorListener.NOOP,
          config.elasticsearch().bulkRequest());

      Metrics.gauge("write.queue",
          "Document events currently buffered in memory.",
          workers, SinkWorkerGroup::getQueueSize);

      Metrics.gauge("es.wait.ms", null, workers, SinkWorkerGroup::getCurrentRequestMillis); // High value indicates the connector has stalled

      // Same as "es.wait.ms" but normalized to seconds for Prometheus
      Metrics.gauge("es.wait.seconds",
          "Duration of in-flight Elasticsearch bulk request (including any retries). Long duration may indicate connector has stalled.",
          workers, value -> value.getCurrentRequestMillis() / (double) SECONDS.toMillis(1));

      final Client dcpClient = DcpHelper.newClient(config.group().name(), config.couchbase(), kvNodes, config.trustStore().orElse(null));

      initEventListener(dcpClient, panicButton, workers::submit);

      final Thread saveCheckpoints = new Thread(checkpointService::save, "save-checkpoints");

      try {
        try {
          dcpClient.connect().block(config.couchbase().dcp().connectTimeout());
        } catch (Throwable t) {
          panicButton.panic("Failed to establish initial DCP connection within " + config.couchbase().dcp().connectTimeout(), t);
        }

        final int numPartitions = dcpClient.numPartitions();
        final Set<Integer> partitions = membership.getPartitions(numPartitions);

        LOGGER.info("Bucket has {} partitions. Membership = {}. Assigned partitions: {}",
            numPartitions,
            membership,
            PartitionSet.from(partitions)
        );

        if (partitions.isEmpty()) {
          // need to do this check, because if we started streaming with an empty list, the DCP client would open streams for *all* partitions
          throw new IllegalArgumentException("There are more workers than Couchbase vbuckets; this worker doesn't have any work to do.");
        }
        checkpointService.init(numPartitions, () -> DcpHelper.getCurrentSeqnosAsMap(dcpClient, partitions, Duration.ofSeconds(5)));

        maybeSetDefaultCheckpoints(config, checkpointDao, bucketUuid, partitions, kvNodes);

        dcpClient.initializeState(StreamFrom.BEGINNING, StreamTo.INFINITY).block();
        initSessionState(dcpClient, checkpointService, partitions);

        // Do quiet period *after* connecting so user doesn't need to wait just to discover
        // configuration problems.
        if (!startupQuietPeriod.isZero()) {
          LOGGER.info("Entering startup quiet period; sleeping for {} so peers can terminate in case of unsafe scaling.", startupQuietPeriod);
          MILLISECONDS.sleep(startupQuietPeriod.toMillis());
          LOGGER.info("Startup quiet period complete.");
        }

        checkpointExecutor.scheduleWithFixedDelay(checkpointService::save, 10, 10, SECONDS);
        RuntimeHelper.addShutdownHook(saveCheckpoints);
        // Unless shutdown is due to panic...
        panicButton.addPrePanicHook(() -> RuntimeHelper.removeShutdownHook(saveCheckpoints));

        try {
          LOGGER.debug("Opening DCP streams for partitions: {}", partitions);
          dcpClient.startStreaming(partitions).block();
        } catch (RuntimeException e) {
          ThrowableHelper.propagateCauseIfPossible(e, InterruptedException.class);
          throw e;
        }

        // Start HTTP server *after* other setup is complete, so the metrics endpoint
        // can be used as a "successful startup" probe.
        httpServer.start();
        if (config.metrics().httpPort() >= 0) {
          LOGGER.info("Prometheus metrics available at http://localhost:{}/metrics/prometheus", httpServer.getBoundPort());
          LOGGER.info("Dropwizard metrics available at http://localhost:{}/metrics/dropwizard?pretty", httpServer.getBoundPort());
        } else {
          LOGGER.info("Metrics HTTP server is disabled. Edit the [metrics] 'httpPort' config property to enable.");
        }

        LOGGER.info("Elasticsearch connector startup complete.");
        onReady.run();

        fatalError = workers.awaitFatalError();
        LOGGER.error("Terminating due to fatal error from worker", fatalError);

      } catch (InterruptedException shutdownRequest) {
        LOGGER.info("Graceful shutdown requested. Saving checkpoints and cleaning up.");
        checkpointService.save();
        throw shutdownRequest;

      } catch (Throwable t) {
        LOGGER.error("Terminating due to fatal error during setup", t);
        throw t;

      } finally {
        // If we get here it means there was a fatal exception, or the connector is running in distributed
        // or test mode and a graceful shutdown was requested. Don't need the shutdown hook for any of those cases.
        RuntimeHelper.removeShutdownHook(saveCheckpoints);

        checkpointExecutor.shutdown();
        metricReporter.stop();
        dcpClient.disconnect().block();
        workers.close(); // to avoid buffer leak, must close *after* dcp client stops feeding it events
        checkpointExecutor.awaitTermination(10, SECONDS);
        cluster.disconnect();
      }
    }

    MILLISECONDS.sleep(500); // give stdout a chance to quiet down so the stack trace on stderr isn't interleaved with stdout.
    throw fatalError;
  }

  /**
   * Set any missing checkpoints to the default value.
   */
  private static void maybeSetDefaultCheckpoints(
      ConnectorConfig config,
      CheckpointDao checkpointDao,
      String bucketUuid,
      Set<Integer> paritionsAssignedToMe,
      Set<SeedNode> kvNodes
  ) {
    Map<Integer, Checkpoint> existingCheckpoints = checkpointDao.loadExisting(bucketUuid, paritionsAssignedToMe);

    if (existingCheckpoints.keySet().equals(paritionsAssignedToMe)) {
      LOGGER.info("defaultCheckpoint: Each assigned partitions has a checkpoint; default not required.");
      return;
    }

    CouchbaseConfig.DefaultCheckpoint defaultCheckpoint = config.couchbase().defaultCheckpoint();

    Map<Integer, Checkpoint> defaults;
    switch (defaultCheckpoint) {
      case NOW:
        defaults = CheckpointClear.getNowForAllPartitions(config, kvNodes);
        break;

      case ZERO:
        defaults = new HashMap<>();
        paritionsAssignedToMe.forEach(p -> defaults.put(p, Checkpoint.ZERO));
        break;

      default:
        throw new RuntimeException("Unexpected default checkpoint type: " + defaultCheckpoint);
    }

    defaults.keySet().retainAll(paritionsAssignedToMe); // narrow to just the partitions assigned to this member
    defaults.keySet().removeAll(existingCheckpoints.keySet()); // don't clobber existing checkpoints

    LOGGER.info(
        "defaultCheckpoint: At least one assigned partition does not have a checkpoint." +
            " Creating '{}' checkpoints for partitions: {}",
        defaultCheckpoint,
        PartitionSet.from(defaults.keySet())
    );

    LOGGER.debug("defaultCheckpoint: Saving '{}' checkpoints: {}", defaultCheckpoint, new TreeMap<>(defaults));
    checkpointDao.save(bucketUuid, defaults);
  }

}
