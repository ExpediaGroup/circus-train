/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
package com.hotels.bdp.circustrain.core;

import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.ReplicaLocationManager;
import com.hotels.bdp.circustrain.api.Replication;
import com.hotels.bdp.circustrain.api.SourceLocationManager;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.copier.CopierContext;
import com.hotels.bdp.circustrain.api.copier.CopierFactory;
import com.hotels.bdp.circustrain.api.copier.CopierFactoryManager;
import com.hotels.bdp.circustrain.api.data.DataManipulator;
import com.hotels.bdp.circustrain.api.data.DataManipulatorFactory;
import com.hotels.bdp.circustrain.api.data.DataManipulatorFactoryManager;
import com.hotels.bdp.circustrain.api.event.CopierListener;
import com.hotels.bdp.circustrain.api.metrics.Metrics;
import com.hotels.bdp.circustrain.api.util.DotJoiner;
import com.hotels.bdp.circustrain.core.replica.Replica;
import com.hotels.bdp.circustrain.core.replica.TableType;
import com.hotels.bdp.circustrain.core.source.Source;

class UnpartitionedTableReplication implements Replication {

  private static final Logger LOG = LoggerFactory.getLogger(UnpartitionedTableReplication.class);

  private final String database;
  private final String table;
  private final Source source;
  private final Replica replica;
  private final String eventId;
  private final CopierFactoryManager copierFactoryManager;
  private Metrics metrics = Metrics.NULL_VALUE;
  private final Map<String, Object> copierOptions;
  private final CopierListener copierListener;
  private final DataManipulatorFactoryManager dataManipulatorFactoryManager;

  private TableReplication tableReplication;

  UnpartitionedTableReplication(
      TableReplication tableReplication,
      Source source,
      Replica replica,
      CopierFactoryManager copierFactoryManager,
      EventIdFactory eventIdFactory,
      Map<String, Object> copierOptions,
      CopierListener copierListener,
      DataManipulatorFactoryManager dataManipulatorFactoryManager) {
    this.tableReplication = tableReplication;
    this.database = tableReplication.getSourceTable().getDatabaseName();
    this.table = tableReplication.getSourceTable().getTableName();
    this.source = source;
    this.replica = replica;
    this.copierFactoryManager = copierFactoryManager;
    this.copierOptions = copierOptions;
    this.copierListener = copierListener;
    this.dataManipulatorFactoryManager = dataManipulatorFactoryManager;
    eventId = eventIdFactory.newEventId(EventIdPrefix.CIRCUS_TRAIN_UNPARTITIONED_TABLE.getPrefix());
  }

  @Override
  public void replicate() throws CircusTrainException {
    try {
      String replicaDatabaseName = tableReplication.getReplicaDatabaseName();
      String replicaTableName = tableReplication.getReplicaTableName();
      replica.validateReplicaTable(replicaDatabaseName, replicaTableName);
      TableAndStatistics sourceTableAndStatistics = source.getTableAndStatistics(database, table);
      Table sourceTable = sourceTableAndStatistics.getTable();
      SourceLocationManager sourceLocationManager = source.getLocationManager(sourceTable, eventId);
      Path sourceLocation = sourceLocationManager.getTableLocation();

      ReplicaLocationManager replicaLocationManager = replica
          .getLocationManager(TableType.UNPARTITIONED, tableReplication.getReplicaTable().getTableLocation(), eventId, sourceLocationManager);
      Path replicaLocation = replicaLocationManager.getTableLocation();

      CopierFactory copierFactory = copierFactoryManager
          .getCopierFactory(sourceLocation, replicaLocation, copierOptions);
      
      CopierContext copierContext = new CopierContext(tableReplication, eventId, sourceLocation, replicaLocation, copierOptions, sourceTable);
      Copier copier = copierFactory.newInstance(copierContext);
      copierListener.copierStart(copier.getClass().getName());
      try {
        metrics = copier.copy();
      } finally {
        copierListener.copierEnd(metrics);
      }
      sourceLocationManager.cleanUpLocations();

      DataManipulatorFactory dataManipulatorFactory = dataManipulatorFactoryManager
          .getFactory(sourceLocation, replicaLocation, copierOptions);
      DataManipulator dataManipulator = dataManipulatorFactory.newInstance(replicaLocation, copierOptions);
      replica.cleanupReplicaTableIfRequired(replicaDatabaseName, replicaTableName, dataManipulator);
      replica
          .updateMetadata(eventId, sourceTableAndStatistics, replicaDatabaseName, replicaTableName,
              replicaLocationManager);
      replicaLocationManager.cleanUpLocations();

      LOG.info("Replicated table {}.{}.", database, table);
    } catch (Throwable t) {
      throw new CircusTrainException("Unable to replicate", t);
    }
  }

  @Override
  public String name() {
    return DotJoiner.join(database, table);
  }

  @Override
  public String getEventId() {
    return eventId;
  }

}
