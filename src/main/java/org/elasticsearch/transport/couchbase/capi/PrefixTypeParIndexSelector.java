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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

/**
 * ex.)
 * #settings
 * 
 * <pre>
 * transport.couchbase.capi.types.type1st: A B C
 * transport.couchbase.capi.types.type2nd: D E F
 * transport.couchbase.capi.types.type3rd: AX DX
 * </pre>
 * <ul>
 * <li>AA0123 -> type1st</li>
 * <li>BB0123 -> type1st</li>
 * <li>CC0123 -> type1st</li>
 * <li>DD0123 -> type2nd</li>
 * <li>EE0123 -> type2nd</li>
 * <li>FF0123 -> type2nd</li>
 * <li>AX0123 -> type3rd</li>
 * <li>AZ0123 -> type1st</li>
 * <li>DX0123 -> type3rd</li>
 * <li>DZ0123 -> type2st</li>
 * <li>XX0123 -> (defaultDocumentType)</li>
 * </ul>
 * 
 * @author madgaoh
 */
public class PrefixTypeParIndexSelector implements TypeSelector {
    protected ESLogger logger = Loggers.getLogger(getClass());

    protected Map<String, SortedMap<Integer, Map<String, String>>> indexTypesMapList;

    /**
     * {@inheritDoc}
     * 
     * @see TypeSelector#init(Settings)
     */
    @Override
    public void init(Settings settings) {
        this.logger = Loggers.getLogger(getClass(), settings);
        Settings typesSettings = settings.getComponentSettings(getClass())
                .getByPrefix("types.");

        indexTypesMapList = new HashMap<String, SortedMap<Integer, Map<String, String>>>();

        String indexNames = typesSettings.get("_index_names", "");

        if (!indexNames.isEmpty()) {
            for (String index : indexNames.split(" ")) {
                Map<String, String> indexTypesMap = typesSettings.getByPrefix(
                        index + ".").getAsMap();
                indexTypesMapList.put(index, makeTypesMapList(indexTypesMap));
            }
            Map<String, String> indexTypesMap = typesSettings.getByPrefix(
                    "_default.").getAsMap();
            indexTypesMapList.put("_default", makeTypesMapList(indexTypesMap));
        } else {
            indexTypesMapList.put("_default",
                    makeTypesMapList(typesSettings.getAsMap()));
        }

        logger.info("typesMapping : {}", indexTypesMapList);
    }

    protected NavigableMap<Integer, Map<String, String>> makeTypesMapList(
            Map<String, String> indexTypesMap) {
        TreeMap<Integer, Map<String, String>> typeMapList = new TreeMap<Integer, Map<String, String>>();

        Set<Entry<String, String>> entrySet = indexTypesMap.entrySet();

        for (Entry<String, String> entry : entrySet) {
            String[] prefixes = entry.getValue().split(" ");
            for (String prefix : prefixes) {
                prefix = prefix.trim();
                Integer length = prefix.length();
                Map<String, String> typeMap = typeMapList.get(length);
                if (typeMap == null) {
                    typeMapList.put(length,
                            typeMap = new HashMap<String, String>());
                }
                String type = entry.getKey();
                String old = typeMap.put(prefix, type);
                if (old != null) {
                    logger.warn(
                            "[{}] type prefix '{}' mapping overwrite from '{}' to '{}'.",
                            getClass().getSimpleName(), prefix, old, type);
                }
            }
        }
        NavigableMap<Integer, Map<String, String>> descendingMap = typeMapList
                .descendingMap();
        return descendingMap;
    }

    /**
     * {@inheritDoc}
     * 
     * @see TypeSelector#getDocumentType(String, String, String)
     */
    @Override
    public String getDocumentType(String database, String docId,
            String defaultType) {

        String type = get(docId, indexTypesMapList.get(database));
        if (type == null) {
            get(docId, indexTypesMapList.get("_default"));
        }
        return type == null ? defaultType : type;
    }

    protected String get(String docId,
            SortedMap<Integer, Map<String, String>> typeMapList) {
        if (typeMapList == null) {
            return null;
        }
        Set<Entry<Integer, Map<String, String>>> entrySet = typeMapList
                .entrySet();
        for (Entry<Integer, Map<String, String>> entry : entrySet) {
            String prefix = docId.substring(0, entry.getKey());
            String type = entry.getValue().get(prefix);
            if (type != null) {
                return type;
            }
        }
        return null;
    }
}
