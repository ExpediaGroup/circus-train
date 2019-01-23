/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.bdp.circustrain.hive.view.transformation;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.conf.TableReplications;

@Profile({ Modules.REPLICATION })
@Component
public class HqlTranslator {

  private static final Splitter DOT_SPLITTER = Splitter.on(".");

  private final Map<String, List<TableTranslation>> mappings;

  private static List<TableTranslation> buildTableTranslations(TableReplication tableReplication) {
    ImmutableList.Builder<TableTranslation> replicaMappings = ImmutableList.<TableTranslation>builder();
    for (Entry<String, String> mapping : tableReplication.getTableMappings().entrySet()) {
      List<String> source = DOT_SPLITTER.splitToList(mapping.getKey());
      if (source.size() != 2) {
        throw new IllegalArgumentException("Original table names must be qualified");
      }
      List<String> replica = DOT_SPLITTER.splitToList(mapping.getValue());
      if (replica.size() != 2) {
        throw new IllegalArgumentException("Translated table names must be qualified");
      }
      replicaMappings.add(new TableTranslation(source.get(0), source.get(1), replica.get(0), replica.get(1)));
    }
    return replicaMappings.build();
  }

  @Autowired
  public HqlTranslator(TableReplications tableReplications) {
    Builder<String, List<TableTranslation>> mappingsBuilder = ImmutableMap.<String, List<TableTranslation>>builder();
    for (TableReplication tableReplication : tableReplications.getTableReplications()) {
      if (tableReplication.getTableMappings() == null || tableReplication.getTableMappings().isEmpty()) {
        continue;
      }
      mappingsBuilder.put(keyFor(tableReplication), buildTableTranslations(tableReplication));
    }
    mappings = mappingsBuilder.build();
  }

  private String keyFor(TableReplication tableReplication) {
    return tableReplication.getSourceTable().getQualifiedName();
  }

  Map<String, List<TableTranslation>> getMappings() {
    return mappings;
  }

  public String translate(String viewQualifiedName, String hql) {
    List<TableTranslation> translations = mappings.get(viewQualifiedName);
    if (translations == null) {
      return hql;
    }
    return translate(hql, translations);
  }

  private String translate(String hql, List<TableTranslation> translations) {
    for (TableTranslation tableTranslation : translations) {
      hql = hql
          // unescaped
          .replaceAll(ignoreCase(tableTranslation.toUnescapedQualifiedOriginalName()),
              tableTranslation.toUnescapedQualifiedReplicaName())
          .replaceAll(ignoreCase(tableTranslation.toUnescapedOriginalTableReference()),
              tableTranslation.toUnescapedReplicaTableReference())
          // escaped
          .replaceAll(ignoreCase(tableTranslation.toEscapedQualifiedOriginalName()),
              tableTranslation.toEscapedQualifiedReplicaName())
          .replaceAll(ignoreCase(tableTranslation.toEscapedOriginalTableReference()),
              tableTranslation.toEscapedReplicaTableReference());
    }
    return hql;
  }

  private static String ignoreCase(String hiveReference) {
    return String.format("(?i)%s", hiveReference.replace(".", "\\."));
  }

}
