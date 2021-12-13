/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.connector.cluster.k8s;

import com.couchbase.connector.cluster.KillSwitch;
import com.couchbase.connector.cluster.PanicButton;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

public class ReplicaChangeWatcher {
  private static final Logger log = LoggerFactory.getLogger(ReplicaChangeWatcher.class);

  private static final Duration resyncPeriod = Duration.ofSeconds(10);

  // panic if this much time passes without a successful resync
  private static final Duration killSwitchDeadline = resyncPeriod
      .multipliedBy(4)
      .plus(Duration.ofSeconds(3));

  private ReplicaChangeWatcher() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Starts watching the replica count and panics if the count changes
   * or can't be determined.
   *
   * @return initial number of replicas
   */
  public static int getReplicasAndPanicOnChange(KubernetesClient client, PanicButton panicButton) {
    try {
      StatefulSetInfo info = StatefulSetInfo.fromHostname();
      String k8sName = info.name;

      log.info("Kubernetes API version = {}", client.discovery().v1().getApiVersion());

      String namespace = client.discovery().v1().getNamespace();
      log.info("Kubernetes namespace = {}", namespace);

      StatefulSet statefulSet = client
          .apps()
          .statefulSets()
          .inNamespace(namespace)
          .withName(k8sName)
          .get();
      requireNonNull(statefulSet, "Failed to get StatefulSet in namespace '" + namespace + "' with name '" + k8sName + "'");

      int initialReplicas = statefulSet.getSpec().getReplicas();
      log.info("StatefulSet replicas = {}", initialReplicas);

      log.info("Kubernetes API resync period = {} ; kill switch deadline = {}",
          resyncPeriod, killSwitchDeadline);

      KillSwitch killSwitch = KillSwitch.start(killSwitchDeadline, () ->
          panicButton.panic("The connector contacts the Kubernetes API server every " +
              resyncPeriod + " to verify the StatefulSet's replica count has not changed," +
              " but " + killSwitchDeadline + " has elapsed without a successful response." +
              " Terminating to ensure multiple pods don't process the same Couchbase partitions."));

      SharedIndexInformer<StatefulSet> informer =
          client.apps()
              .statefulSets()
              .inNamespace(namespace)
              .withName(k8sName)
              .inform(new ResourceEventHandler<>() {
                // When the K8S API server is reachable, an update happens once per resync period
                // and also immediately when the informer detects a change to the resource.
                @Override
                public void onUpdate(StatefulSet oldSet, StatefulSet newSet) {
                  killSwitch.reset();

                  int newReplicas = newSet.getSpec().getReplicas();
                  if (newReplicas != 0 && newReplicas != initialReplicas) {
                    // Panic to terminate the connector and let Kubernetes restart it.
                    // This is simpler than trying to stream from different vbuckets on the fly.
                    panicButton.mildPanic("The connector is automatically restarting because" +
                        " it detected a change to the number of replicas in its StatefulSet." +
                        " This is the intended behavior, and not a cause for alarm." +
                        " Upon restart, the connector will wait for a quiet period to elapse, giving all pods" +
                        " a chance to notice the change and shut down. This prevents multiple pods" +
                        " from processing the same Couchbase partitions." +
                        " There is a [very small] chance this strategy could fail, in which case" +
                        " stale versions of some documents could be written to Elasticsearch and remain" +
                        " until the documents are modified again in Couchbase." +
                        " To be absolutely sure this never happens, we recommend first scaling" +
                        " the StatefulSet down to zero (or deleting it) and waiting for pods" +
                        " to terminate before scaling back up to the desired number of replicas.");
                  }
                }

                @Override
                public void onAdd(StatefulSet it) {
                }

                @Override
                public void onDelete(StatefulSet it, boolean deletedFinalStateUnknown) {
                }
              }, resyncPeriod.toMillis());

      return initialReplicas;

    } catch (Throwable t) {
      panicButton.panic("Failed to get/watch StatefulSet replica count.", t);
      throw t; // unreachable
    }
  }

  public static Duration startupQuietPeriod() {
    return killSwitchDeadline.plus(Duration.ofSeconds(10));
  }
}
