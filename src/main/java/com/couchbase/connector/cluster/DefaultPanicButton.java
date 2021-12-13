/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.connector.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static java.util.Objects.requireNonNull;

public class DefaultPanicButton implements PanicButton {
  private static final Logger log = LoggerFactory.getLogger(DefaultPanicButton.class);

  private final List<Runnable> prePanicHooks = new CopyOnWriteArrayList<>();

  @Override
  public void addPrePanicHook(Runnable hook) {
    prePanicHooks.add(requireNonNull(hook));
  }

  private void runPrePanicHooks() {
    try {
      List<Runnable> hooks = new ArrayList<>(prePanicHooks);
      Collections.reverse(hooks);
      hooks.forEach(it -> {
        try {
          it.run();
        } catch (Throwable t) {
          log.error("Pre-panic hook threw exception.", t);
        }
      });
    } catch (Throwable t) {
      log.error("Failed to invoke pre-panic hooks.", t);
    }
  }

  public void mildPanic(String message) {
    try {
      runPrePanicHooks();

      if (runningInKubernetes()) {
        writeTerminationMessage(message);
      }

      log.warn("*** TERMINATING: {}", message);

    } finally {
      System.exit(1);
    }
  }

  @Override
  public void panic(String message, Throwable t) {
    //noinspection finally
    try {
      runPrePanicHooks();

      if (runningInKubernetes()) {
        writeTerminationMessage(t == null ? message : message + "\n" + getStackTraceAsString(t));
      }

      log.error("PANIC: {}", message, t);

    } finally {
      System.exit(1);
    }
  }

  private static boolean runningInKubernetes() {
    return System.getenv("KUBERNETES_SERVICE_HOST") != null;
  }

  private static void writeTerminationMessage(String msg) {
    String path = "/dev/termination-log";
    try (Writer w = new OutputStreamWriter(new FileOutputStream(path))) {
      w.write(msg + "\n");
    } catch (Exception e) {
      log.warn("Failed to write termination message to {}", path, e);
    }
  }
}
