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

package com.couchbase.connector.cluster.consul.rpc;

import com.google.common.base.Throwables;

public class RpcResult<T> {
  private final T result; // nullable
  private final Throwable failure; // nullable

  public static <T> RpcResult<T> newSuccess(T value) {
    return new RpcResult<>(value, null);
  }

  public static <Void> RpcResult<Void> newSuccess() {
    return new RpcResult<>(null, null);
  }

  public static <T> RpcResult<T> newFailure(Throwable t) {
    return new RpcResult<>(null, t);
  }

  private RpcResult(T result, Throwable failure) {
    this.result = result;
    this.failure = failure;
  }

  public T get() {
    if (failure != null) {
      Throwables.throwIfUnchecked(failure);
      throw new RuntimeException(failure);
    }
    return result;
  }

  public boolean isFailed() {
    return failure != null;
  }

  @Override
  public String toString() {
    return "RpcResult{" +
        "result=" + result +
        ", failure=" + failure +
        '}';
  }
}
