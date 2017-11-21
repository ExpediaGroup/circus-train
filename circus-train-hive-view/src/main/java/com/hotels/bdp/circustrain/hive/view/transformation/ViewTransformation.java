/**
 * Copyright (C) 2016-2017 Expedia Inc.
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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.metadata.TableTransformation;
import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;
import com.hotels.bdp.circustrain.hive.parser.HiveLanguageParser;

@Profile({ Modules.REPLICATION })
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ViewTransformation implements TableTransformation {
  private static final Logger LOG = LoggerFactory.getLogger(ViewTransformation.class);

  @VisibleForTesting
  final static String SKIP_TABLE_EXIST_CHECKS = "com.hotels.bdp.circustrain.hive.view.transformation.ViewTransformation.skip_table_exist_checks";

  private final HiveConf replicaHiveConf;
  private final HqlTranslator hqlTranslator;
  private final Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier;

  @Autowired
  public ViewTransformation(
      HiveConf replicaHiveConf,
      HqlTranslator hqlTranslator,
      Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier) {
    this.replicaHiveConf = replicaHiveConf;
    this.hqlTranslator = hqlTranslator;
    this.replicaMetaStoreClientSupplier = replicaMetaStoreClientSupplier;
  }

  @Override
  public Table transform(Table table) {
    if (!MetaStoreUtils.isView(table)) {
      return table;
    }

    LOG.info("Translating HQL of view {}.{}", table.getDbName(), table.getTableName());
    String tableQualifiedName = Warehouse.getQualifiedName(table);
    String hql = hqlTranslator.translate(tableQualifiedName, table.getViewOriginalText());
    String expandedHql = hqlTranslator.translate(tableQualifiedName, table.getViewExpandedText());

    Table transformedView = new Table(table);
    transformedView.setViewOriginalText(hql);
    transformedView.setViewExpandedText(expandedHql);

    if (!replicaHiveConf.getBoolean(SKIP_TABLE_EXIST_CHECKS, false)) {
      LOG.info("Validating that tables used by the view {}.{} exist in the replica catalog", table.getDbName(),
          table.getTableName());
      validateReferencedTables(transformedView);
    }

    return transformedView;
  }

  private void validateReferencedTables(Table view) {
    TableProcessor processor = new TableProcessor();
    HiveLanguageParser parser = new HiveLanguageParser(new HiveConf());
    parser.parse(view.getViewExpandedText(), processor);

    try (CloseableMetaStoreClient replicaClient = replicaMetaStoreClientSupplier.get()) {
      for (String replicaTable : processor.getTables()) {
        String[] nameParts = MetaStoreUtils.getQualifiedName(null, replicaTable);
        try {
          replicaClient.getTable(nameParts[0], nameParts[1]);
        } catch (NoSuchObjectException e) {
          String message = String.format("Table or view %s does not exists in replica catalog", replicaTable);
          throw new CircusTrainException(message, e);
        } catch (Exception e) {
          String message = String.format("Unable to validate tables used by view %s.%s", view.getDbName(),
              view.getTableName());
          throw new CircusTrainException(message, e);
        }
      }
    }
  }

}
