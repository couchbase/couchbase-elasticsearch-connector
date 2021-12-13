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

package com.couchbase.connector.cluster;

/**
 * The big red button that brings everything to a crashing halt.
 */
public interface PanicButton {
  default void panic(String message) {
    panic(message, null);
  }

  /**
   * Same as a panic, but log as WARN instead of ERROR and
   * don't say anything about panicking in the log message.
   */
  void mildPanic(String message);

  void panic(String message, Throwable t);

  void addPrePanicHook(Runnable hook);
}
