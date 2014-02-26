/**
 * Copyright (c) 2013 atWare, Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package org.elasticsearch.transport.couchbase.capi;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

public class SampleJsonCustomizer implements JsonCustomizer {
    protected ESLogger logger;

    protected Map<String, String> clsTypeMap;

    public void init(Settings settings) {
        this.logger = Loggers.getLogger(getClass(), settings);
    }

    public Object customize(String database, String docId, String rev,
            Map<String, Object> json) {
        Map<String, Object> msg = new HashMap<String, Object>();
        if (!json.containsKey("addField")) {
            json.put("addField", new Date());
            msg.put("add", "addField");
        }
        if (json.containsKey("removeField")) {
            json.put("removeField", null);
            msg.put("remove", "removeField");
        }

        if (msg.isEmpty()) {
            return null;
        }
        msg.put("customizer", "SampleJsonCustomizer");
        return msg;
    }
}
