/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.connector.cluster.consul;

import com.google.common.base.Throwables;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.io.Closeable;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

public class ReactorHelper {
  private ReactorHelper() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Wraps the given Disposable in a Closeable for use in try-with-resources blocks.
   */
  public static Closeable asCloseable(Disposable d) {
    return d::dispose;
  }

  /**
   * Subscribes to the flux and blocks until an item matching the predicate is emitted
   * or the thread is interrupted.
   *
   * @return The first item matching the predicate, or {@code null} if the flux completes without a match.
   * @throws InterruptedException if the thread is interrupted.
   */
  public static <T> T await(Flux<T> flux, Predicate<? super T> condition)
      throws InterruptedException {

    try {
      return flux.filter(condition)
          .blockFirst();

    } catch (RuntimeException e) {
      propagateIfInterrupted(e);
      throw e;
    }
  }

  /**
   * Subscribes to the flux and blocks until an item matching the predicate is emitted,
   * thread is interrupted, or the timeout expires.
   *
   * @return The first item matching the predicate, or {@code null} if the flux completes without a match.
   * @throws InterruptedException if the thread is interrupted.
   * @throws TimeoutException     if the flux does not emit a matching item before the timeout expires.
   */
  public static <T> T await(Flux<T> flux, Predicate<? super T> condition, Duration timeout)
      throws InterruptedException, TimeoutException {

    try {
      // Avoid blockFirst(Duration) because it throws an ambiguous IllegalStateException
      // instead of a wrapped TimeoutException.
      return flux.filter(condition)
          .timeout(timeout)
          .blockFirst();

    } catch (RuntimeException e) {
      propagateIfInterrupted(e);
      propagateIfTimeout(e);
      throw e;
    }
  }

  private static void propagateIfInterrupted(RuntimeException e) throws InterruptedException {
    Throwables.propagateIfPossible(e.getCause(), InterruptedException.class);
    if (e.getCause() instanceof InterruptedIOException) {
      throw addSuppressedCause(new InterruptedException(e.getMessage()), e);
    }
  }

  private static void propagateIfTimeout(RuntimeException e) throws TimeoutException {
    Throwables.propagateIfPossible(e.getCause(), TimeoutException.class);
  }

  /**
   * Adds the "cause" to the list of exception suppressed by the given exception.
   * For use when converting an exception to a type whose constructor does not accept a cause.
   */
  private static <T extends Throwable> T addSuppressedCause(T exception, Throwable cause) {
    exception.addSuppressed(cause);
    return exception;
  }
}
