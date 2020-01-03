/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.connector.util;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Runs shutdown hooks with a timeout.
 * <p>
 * Prevents stuck hooks from delaying or preventing JVM termination.
 */
public class RuntimeHelper {
  private RuntimeHelper() {
    throw new AssertionError("not instantiable");
  }

  private static final Duration shutdownHookTimeout = Duration.ofSeconds(3);

  private static final List<Thread> managedShutdownHooks = new ArrayList<>();

  static {
    // Register an "umbrella" hook that starts the other hooks
    // and halts the JVM if they take too long.
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        synchronized (managedShutdownHooks) {
          for (Thread t : managedShutdownHooks) {
            t.start();
          }

          final long deadlineNanos = System.nanoTime() + shutdownHookTimeout.toNanos();

          for (Thread t : managedShutdownHooks) {
            // prevent joining for 0 milliseconds, which would mean "wait forever."
            final long remainingMillis = Math.max(1, NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));
            t.join(remainingMillis);
            if (t.isAlive()) {
              // logging has likely been shut down by this point, so use stderr
              System.err.println("Shutdown hook failed to terminate within " + shutdownHookTimeout + " : " + t);
              halt();
            }
          }
        }
      } catch (Throwable t) {
        // logging has likely been shut down by this point, so use stderr
        t.printStackTrace();
        halt();
      }
    }));
  }

  /**
   * Immediately terminate the JVM process, without running any shutdown hooks or finalizers.
   */
  private static void halt() {
    // logging has likely been shut down by this point, so use stderr
    System.err.println("Halting.");
    Runtime.getRuntime().halt(-1);
  }

  public static void addShutdownHook(Thread hook) {
    requireNonNull(hook);
    synchronized (managedShutdownHooks) {
      managedShutdownHooks.add(hook);
    }
  }

  public static boolean removeShutdownHook(Thread hook) {
    requireNonNull(hook);
    synchronized (managedShutdownHooks) {
      return managedShutdownHooks.remove(hook);
    }
  }
}
