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

package com.couchbase.connector.util;

import com.google.common.base.Throwables;

import java.util.Optional;

public class ThrowableHelper {
  /**
   * If the given Throwable's causal chain includes an instance of the given type,
   * throw the matching instance.
   */
  public static <T extends Throwable> void propagateCauseIfPossible(Throwable t, Class<T> type) throws T {
    propagateIfPresent(findCause(t, type));
  }

  /**
   * Returns true if the given Throwable's causal chain includes an instance of the given type.
   */
  public static <T extends Throwable> boolean hasCause(Throwable t, Class<T> type) {
    return findCause(t, type).isPresent();
  }

  private static <T extends Throwable> void propagateIfPresent(Optional<T> t) throws T {
    if (t.isPresent()) {
      throw t.get();
    }
  }

  private static <T extends Throwable> Optional<T> findCause(Throwable t, Class<T> type) {
    for (Throwable cause : Throwables.getCausalChain(t)) {
      if (type.isAssignableFrom(cause.getClass())) {
        return Optional.of(type.cast(cause));
      }
    }
    return Optional.empty();
  }

  public static String formatMessageWithStackTrace(String message, Throwable t) {
    return message + System.lineSeparator() + Throwables.getStackTraceAsString(t);
  }
}
