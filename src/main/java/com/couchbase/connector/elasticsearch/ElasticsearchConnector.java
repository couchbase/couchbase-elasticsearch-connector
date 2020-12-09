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
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.util.Version;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.connector.cluster.Coordinator;
import com.couchbase.connector.cluster.Membership;
import com.couchbase.connector.cluster.StaticCoordinator;
import com.couchbase.connector.config.ConfigException;
import com.couchbase.connector.config.es.ConnectorConfig;
import com.couchbase.connector.config.es.ElasticsearchConfig;
import com.couchbase.connector.config.es.TypeConfig;
import com.couchbase.connector.dcp.CheckpointDao;
import com.couchbase.connector.dcp.CheckpointService;
import com.couchbase.connector.dcp.CouchbaseCheckpointDao;
import com.couchbase.connector.dcp.CouchbaseHelper;
import com.couchbase.connector.dcp.DcpHelper;
import com.couchbase.connector.elasticsearch.cli.AbstractCliCommand;
import com.couchbase.connector.elasticsearch.io.RequestFactory;
import com.couchbase.connector.util.HttpServer;
import com.couchbase.connector.util.RuntimeHelper;
import com.couchbase.connector.util.ThrowableHelper;
import joptsimple.OptionSet;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.connector.VersionHelper.getVersionString;
import static com.couchbase.connector.dcp.CouchbaseHelper.requireCouchbaseVersion;
import static com.couchbase.connector.dcp.DcpHelper.getCurrentSeqnos;
import static com.couchbase.connector.dcp.DcpHelper.initEventListener;
import static com.couchbase.connector.dcp.DcpHelper.initSessionState;
import static com.couchbase.connector.elasticsearch.ElasticsearchHelper.newElasticsearchClient;
import static com.couchbase.connector.elasticsearch.ElasticsearchHelper.waitForElasticsearchAndRequireVersion;
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

  private static Slf4jReporter newSlf4jReporter(TimeValue logInterval) {
    Slf4jReporter reporter = Slf4jReporter.forRegistry(Metrics.dropwizardRegistry())
        .convertDurationsTo(MILLISECONDS)
        .convertRatesTo(SECONDS)
        .outputTo(LoggerFactory.getLogger("cbes.metrics"))
        .withLoggingLevel(Slf4jReporter.LoggingLevel.INFO)
        .build();
    if (logInterval.duration() > 0) {
      reporter.start(logInterval.duration(), logInterval.duration(), logInterval.timeUnit());
    }
    return reporter;
  }

  public static void main(String... args) throws Throwable {
    LOGGER.info("Couchbase Elasticsearch Connector version {}", getVersionString());

    final OptionsParser parser = new OptionsParser();
    final OptionSet options = parser.parse(args);

    final File configFile = options.valueOf(parser.configFile);
    System.out.println("Reading connector configuration from " + configFile.getAbsoluteFile());
    final ConnectorConfig config = ConnectorConfig.from(configFile);
    run(config);
  }

  public static void run(ConnectorConfig config) throws Throwable {
    final Throwable fatalError;

    final Membership membership = config.group().staticMembership();

    final Coordinator coordinator = new StaticCoordinator();

    LOGGER.info("Read configuration: {}", redactSystem(config));

    final ScheduledExecutorService checkpointExecutor = Executors.newSingleThreadScheduledExecutor();

    try (Slf4jReporter metricReporter = newSlf4jReporter(config.metrics().logInterval());
         HttpServer httpServer = new HttpServer(config.metrics().httpPort());
         RestHighLevelClient esClient = newElasticsearchClient(config.elasticsearch(), config.trustStore())) {

      httpServer.start();
      if (config.metrics().httpPort() >= 0) {
        LOGGER.info("Metrics available at http://localhost:{}/metrics?pretty", httpServer.getBoundPort());
      } else {
        LOGGER.info("Metrics HTTP server is disabled. Edit the [metrics] 'httpPort' config property to enable.");
      }

      final ClusterEnvironment env = CouchbaseHelper.environmentBuilder(config.couchbase(), config.trustStore()).build();
      final Cluster cluster = CouchbaseHelper.createCluster(config.couchbase(), env);

      final Version elasticsearchVersion = waitForElasticsearchAndRequireVersion(
          esClient, new Version(2, 0, 0), new Version(5, 6, 16));
      LOGGER.info("Elasticsearch version {}", elasticsearchVersion);

      validateConfig(elasticsearchVersion, config.elasticsearch());

      // Wait for couchbase server to come online, then open the bucket.
      final Bucket bucket = CouchbaseHelper.waitForBucket(cluster, config.couchbase().bucket());
      final Set<SeedNode> kvNodes = CouchbaseHelper.getKvNodes(config.couchbase(), bucket);

      final boolean storeMetadataInSourceBucket = config.couchbase().metadataBucket().equals(config.couchbase().bucket());
      final Bucket metadataBucket = storeMetadataInSourceBucket ? bucket : CouchbaseHelper.waitForBucket(cluster, config.couchbase().metadataBucket());
      final Collection metadataCollection = CouchbaseHelper.getMetadataCollection(metadataBucket, config.couchbase());

      // Do this after waiting for the bucket, because waitForBucket has nicer retry backoff.
      // Checkpoint metadata is stored using Extended Attributes, a feature introduced in 5.0.
      LOGGER.info("Couchbase Server version {}", requireCouchbaseVersion(cluster, new Version(5, 0, 0)));

      final CheckpointDao checkpointDao = new CouchbaseCheckpointDao(metadataCollection, config.group().name());

      final String bucketUuid = ""; // todo get this from dcp client
      final CheckpointService checkpointService = new CheckpointService(bucketUuid, checkpointDao);
      final RequestFactory requestFactory = new RequestFactory(
          config.elasticsearch().types(), config.elasticsearch().docStructure(), config.elasticsearch().rejectLog());

      final ElasticsearchWorkerGroup workers = new ElasticsearchWorkerGroup(
          esClient,
          checkpointService,
          requestFactory,
          ErrorListener.NOOP,
          config.elasticsearch().bulkRequest());

      Metrics.gauge("writeQueue", workers, ElasticsearchWorkerGroup::getQueueSize);
      Metrics.gauge("esWaitMs", workers, ElasticsearchWorkerGroup::getCurrentRequestMillis); // High value indicates the connector has stalled

      final Client dcpClient = DcpHelper.newClient(config.group().name(), config.couchbase(), kvNodes, config.trustStore());

      initEventListener(dcpClient, coordinator, workers::submit);

      final Thread saveCheckpoints = new Thread(checkpointService::save, "save-checkpoints");

      try {
        try {
          dcpClient.connect().block(Duration.ofMillis(config.couchbase().dcp().connectTimeout().millis()));
        } catch (Exception e) {
          LOGGER.error("Failed to establish initial DCP connection within {} -- shutting down.", config.couchbase().dcp().connectTimeout(), e);
          System.exit(1);
        }

        final int numPartitions = dcpClient.numPartitions();
        LOGGER.info("Bucket has {} partitions. Membership = {}", numPartitions, membership);
        final Set<Integer> partitions = membership.getPartitions(numPartitions);
        if (partitions.isEmpty()) {
          // need to do this check, because if we started streaming with an empty list, the DCP client would open streams for *all* partitions
          throw new IllegalArgumentException("There are more workers than Couchbase vbuckets; this worker doesn't have any work to do.");
        }
        checkpointService.init(getCurrentSeqnos(dcpClient, partitions),
            () -> DcpHelper.getCurrentSeqnosAsMap(dcpClient, partitions, Duration.ofSeconds(5)));

        dcpClient.initializeState(StreamFrom.BEGINNING, StreamTo.INFINITY).block();
        initSessionState(dcpClient, checkpointService, partitions);

        checkpointExecutor.scheduleWithFixedDelay(checkpointService::save, 10, 10, SECONDS);
        RuntimeHelper.addShutdownHook(saveCheckpoints);

        try {
          LOGGER.debug("Opening DCP streams for partitions: {}", partitions);
          dcpClient.startStreaming(partitions).block();
        } catch (RuntimeException e) {
          ThrowableHelper.propagateCauseIfPossible(e, InterruptedException.class);
          throw e;
        }

        fatalError = workers.awaitFatalError();
        LOGGER.error("Terminating due to fatal error.", fatalError);

      } catch (InterruptedException shutdownRequest) {
        LOGGER.info("Graceful shutdown requested. Saving checkpoints and cleaning up.");
        checkpointService.save();
        throw shutdownRequest;

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
        env.shutdown(); // can't reuse, because connector config might have different SSL settings next time
      }
    }

    MILLISECONDS.sleep(500); // give stdout a chance to quiet down so the stack trace on stderr isn't interleaved with stdout.
    throw fatalError;
  }

  private static void validateConfig(Version elasticsearchVersion, ElasticsearchConfig config) {
    // The default/example config is for Elasticsearch 6, and isn't 100% compatible with ES 5.x.
    // Rather than spamming the log with indexing errors, let's do a preflight check.
    if (elasticsearchVersion.major() < 6) {
      for (TypeConfig type : config.types()) {
        if (type.type().startsWith("_")) {
          throw new ConfigException(
              "Elasticsearch versions prior to 6.0 do not allow type names to start with underscores. " +
                  "Please edit the connector configuration and replace type name '" + type.type() + "' with something else.");
        }
      }
    }
  }
}
