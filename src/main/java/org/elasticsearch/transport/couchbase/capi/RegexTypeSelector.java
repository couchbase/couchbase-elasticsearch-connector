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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

public class RegexTypeSelector implements TypeSelector {

    protected ESLogger logger;

    protected Map<String, Pattern> documentTypePatterns;

    public void init(Settings settings) {
        this.logger = Loggers.getLogger(getClass(), settings);
        this.documentTypePatterns = new HashMap<String, Pattern>();
        Map<String, String> documentTypePatternStrings = settings.getByPrefix(
                "couchbase.documentTypes.").getAsMap();
        for (String key : documentTypePatternStrings.keySet()) {
            String pattern = documentTypePatternStrings.get(key);
            logger.info("See document type: {} with pattern: {} compiling...",
                    key, pattern);
            documentTypePatterns.put(key, Pattern.compile(pattern));
        }
    }

    public String getDocumentType(String database, String docId,
            String defaultType) {
        for (Entry<String, Pattern> typePattern : documentTypePatterns
                .entrySet()) {
            if (typePattern.getValue().matcher(docId).matches()) {
                return typePattern.getKey();
            }
        }
        return defaultType;
    }
}
