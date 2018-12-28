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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticCoordinator implements Coordinator {
  private static final Logger LOGGER = LoggerFactory.getLogger(StaticCoordinator.class);

  @Override
  public void panic(String message, Throwable t) {
    LOGGER.error("PANIC: " + message, t);

    // todo: think a little harder and exit some other way?
    System.exit(1);
  }
}
