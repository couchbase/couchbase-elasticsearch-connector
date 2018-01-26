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

package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;

/**
 * Utility methods whose implementations differ between the plugin branches
 * so that most of the changes are isolated to one file.
 */
class CompatibilityHelper {
    private CompatibilityHelper() {
        throw new AssertionError("not instantiable");
    }

    static void setPipeline(IndexRequestBuilder indexBuilder, PluginSettings pluginSettings) {
        // not supported in alder branch
        //indexBuilder.setPipeline(pluginSettings.getPipeline());
    }

    static boolean isCreated(IndexResponse response) {
        return response.isCreated();
    }
}
