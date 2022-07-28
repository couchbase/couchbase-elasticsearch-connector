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

package com.couchbase.connector.elasticsearch.io;

import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.couchbase.connector.dcp.Event;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.slf4j.Logger;

import java.util.List;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static java.util.Objects.requireNonNull;

/**
 * Aggregates and summarizes failures we intend to retry.
 */
class RetryReporter {
  private final ListMultimap<String, String> errorMessageToEvents = ArrayListMultimap.create();
  private final Logger logger;

  private RetryReporter(Logger logger) {
    this.logger = requireNonNull(logger);
  }

  static RetryReporter forLogger(Logger logger) {
    return new RetryReporter(logger);
  }

  void add(Event e, BulkResponseItem failure) {
    if (!logger.isInfoEnabled()) {
      return;
    }
    ErrorCause cause = failure.error();
    if (cause == null) {
      throw new IllegalArgumentException("bulk response item did not fail");
    }

    final String message = "status=" + failure.status() + " message=" + cause.reason();
    errorMessageToEvents.put(message, redactUser(e).toString());
  }

  void report() {
    if (!logger.isInfoEnabled()) {
      return;
    }

    for (String errorMessage : errorMessageToEvents.keySet()) {
      final List<String> events = errorMessageToEvents.get(errorMessage);
      String message = "Retrying " + events.get(0);
      if (events.size() > 1) {
        message += " (and " + (events.size() - 1) + " others)";
      }
      message += " due to: " + errorMessage;

      logger.info(message);
    }
  }
}
