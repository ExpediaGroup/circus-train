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
import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.copier.CopierFactory;
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
  private final String targetTableLocation;
  private final String replicaDatabaseName;
  private final String replicaTableName;
  private Metrics metrics = Metrics.NULL_VALUE;
  private final Map<String, Object> copierOptions;

  private final CopierListener copierListener;

  UnpartitionedTableReplication(
      String database,
      String table,
      Source source,
      Replica replica,
      CopierFactoryManager copierFactoryManager,
      EventIdFactory eventIdFactory,
      String targetTableLocation,
      String replicaDatabaseName,
      String replicaTableName,
      Map<String, Object> copierOptions,
      CopierListener copierListener) {
    this.database = database;
    this.table = table;
    this.source = source;
    this.replica = replica;
    this.copierFactoryManager = copierFactoryManager;
    this.targetTableLocation = targetTableLocation;
    this.replicaDatabaseName = replicaDatabaseName;
    this.replicaTableName = replicaTableName;
    this.copierOptions = copierOptions;
    this.copierListener = copierListener;
    eventId = eventIdFactory.newEventId("ctt");
  }

  @Override
  public void replicate() throws CircusTrainException {
    try {
      replica.validateReplicaTable(replicaDatabaseName, replicaTableName);
      TableAndStatistics sourceTableAndStatistics = source.getTableAndStatistics(database, table);
      Table sourceTable = sourceTableAndStatistics.getTable();
      SourceLocationManager sourceLocationManager = source.getLocationManager(sourceTable, eventId);
      Path sourceLocation = sourceLocationManager.getTableLocation();

      ReplicaLocationManager replicaLocationManager = replica.getLocationManager(TableType.UNPARTITIONED,
          targetTableLocation, eventId, sourceLocationManager);
      Path replicaLocation = replicaLocationManager.getTableLocation();

      CopierFactory copierFactory = copierFactoryManager.getCopierFactory(sourceLocation, replicaLocation, copierOptions);
      Copier copier = copierFactory.newInstance(eventId, sourceLocation, replicaLocation, copierOptions);
      copierListener.copierStart(copier.getClass().getName());
      try {
        metrics = copier.copy();
      } finally {
        copierListener.copierEnd(metrics);
      }
      sourceLocationManager.cleanUpLocations();

      replica.updateMetadata(eventId, sourceTableAndStatistics, replicaDatabaseName, replicaTableName,
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
