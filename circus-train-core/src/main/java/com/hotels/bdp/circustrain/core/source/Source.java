/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
package com.hotels.bdp.circustrain.core.source;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.SourceLocationManager;
import com.hotels.bdp.circustrain.api.copier.CopierOptions;
import com.hotels.bdp.circustrain.api.event.SourceCatalogListener;
import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;
import com.hotels.bdp.circustrain.core.HiveEndpoint;
import com.hotels.bdp.circustrain.core.PartitionsAndStatistics;
import com.hotels.bdp.circustrain.core.TableAndStatistics;
import com.hotels.bdp.circustrain.core.conf.SourceCatalog;
import com.hotels.bdp.circustrain.core.conf.SourceTable;
import com.hotels.bdp.circustrain.core.conf.TableReplication;
import com.hotels.bdp.circustrain.core.event.EventUtils;

public class Source extends HiveEndpoint {

  private final String sourceTableLocation;
  private final boolean snapshotsDisabled;
  private final SourceCatalogListener sourceCatalogListener;

  /**
   * Use {@link SourceFactory}
   */
  Source(
      SourceCatalog sourceCatalog,
      HiveConf sourceHiveConf,
      Supplier<CloseableMetaStoreClient> sourceMetaStoreClientSupplier,
      SourceCatalogListener sourceCatalogListener,
      boolean snapshotsDisabled,
      String sourceTableLocation) {
    super(sourceCatalog.getName(), sourceHiveConf, sourceMetaStoreClientSupplier);
    this.sourceTableLocation = sourceTableLocation;
    this.sourceCatalogListener = sourceCatalogListener;
    this.snapshotsDisabled = snapshotsDisabled;
  }

  @Override
  public TableAndStatistics getTableAndStatistics(String database, String table) {
    TableAndStatistics sourceTable = super.getTableAndStatistics(database, table);
    sourceCatalogListener.resolvedMetaStoreSourceTable(EventUtils.toEventTable(sourceTable.getTable()));
    return sourceTable;
  }

  @Override
  public PartitionsAndStatistics getPartitions(Table sourceTable, String partitionPredicate, int maxPartitions)
    throws TException {
    PartitionsAndStatistics sourcePartitions = super.getPartitions(sourceTable, partitionPredicate, maxPartitions);
    sourceCatalogListener.resolvedSourcePartitions(EventUtils.toEventPartitions(sourceTable, sourcePartitions.getPartitions()));
    return sourcePartitions;
  }

  public SourceLocationManager getLocationManager(Table table, String eventId) throws IOException {
    if (MetaStoreUtils.isView(table)) {
      return new ViewLocationManager();
    }
    return new HdfsSnapshotLocationManager(getHiveConf(), eventId, table, snapshotsDisabled, sourceTableLocation,
        sourceCatalogListener);
  }

  public SourceLocationManager getLocationManager(
      Table table,
      List<Partition> partitions,
      String eventId,
      Map<String, Object> copierOptions)
    throws IOException {
    if (MetaStoreUtils.isView(table)) {
      return new ViewLocationManager();
    }
    HdfsSnapshotLocationManager hdfsSnapshotLocationManager = new HdfsSnapshotLocationManager(getHiveConf(), eventId,
        table, partitions, snapshotsDisabled, sourceTableLocation, sourceCatalogListener);
    boolean ignoreMissingFolder = MapUtils.getBooleanValue(copierOptions,
        CopierOptions.IGNORE_MISSING_PARTITION_FOLDER_ERRORS, false);
    if (ignoreMissingFolder) {
      return new FilterMissingPartitionsLocationManager(hdfsSnapshotLocationManager, getHiveConf());
    }
    return hdfsSnapshotLocationManager;
  }

  @Override
  public TableAndStatistics getTableAndStatistics(TableReplication tableReplication) {
    SourceTable sourceTable = tableReplication.getSourceTable();
    return super.getTableAndStatistics(sourceTable.getDatabaseName(), sourceTable.getTableName());
  }

  @VisibleForTesting
  boolean isSnapshotsDisabled() {
    return snapshotsDisabled;
  }
}
