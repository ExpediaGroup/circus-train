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
package com.hotels.bdp.circustrain.core;

import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.ReplicaLocationManager;
import com.hotels.bdp.circustrain.api.Replication;
import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.bdp.circustrain.api.util.DotJoiner;
import com.hotels.bdp.circustrain.core.replica.InvalidReplicationModeException;
import com.hotels.bdp.circustrain.core.replica.MetadataUpdateReplicaLocationManager;
import com.hotels.bdp.circustrain.core.replica.Replica;
import com.hotels.bdp.circustrain.core.replica.TableType;
import com.hotels.bdp.circustrain.core.source.Source;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class UnpartitionedTableMetadataUpdateReplication implements Replication {

  private static final Logger LOG = LoggerFactory.getLogger(UnpartitionedTableReplication.class);

  private final String database;
  private final String table;
  private final Source source;
  private final Replica replica;
  private final String eventId;
  private final String replicaDatabaseName;
  private final String replicaTableName;

  UnpartitionedTableMetadataUpdateReplication(
      String database,
      String table,
      Source source,
      Replica replica,
      EventIdFactory eventIdFactory,
      String replicaDatabaseName,
      String replicaTableName) {
    this.database = database;
    this.table = table;
    this.source = source;
    this.replica = replica;
    this.replicaDatabaseName = replicaDatabaseName;
    this.replicaTableName = replicaTableName;
    eventId = eventIdFactory.newEventId(EventIdPrefix.CIRCUS_TRAIN_UNPARTITIONED_TABLE.getPrefix());
  }

  @Override
  public void replicate() throws CircusTrainException {
    try {
      replica.validateReplicaTable(replicaDatabaseName, replicaTableName);
      try (CloseableMetaStoreClient client = replica.getMetaStoreClientSupplier().get()) {
        String previousLocation = getPreviousLocation(client);
        // using previous eventId because we shouldn't override the path
        ReplicaLocationManager replicaLocationManager = new MetadataUpdateReplicaLocationManager(client,
            TableType.UNPARTITIONED, previousLocation, replicaDatabaseName, replicaTableName);
        TableAndStatistics sourceTableAndStatistics = source.getTableAndStatistics(database, table);
        replica
            .updateMetadata(eventId, sourceTableAndStatistics, replicaDatabaseName, replicaTableName,
                replicaLocationManager);
      }
      LOG.info("Metadata updated for table {}.{} (no data copied).", database, table);
    } catch (Throwable t) {
      throw new CircusTrainException("Unable to replicate", t);
    }
  }

  private String getPreviousLocation(CloseableMetaStoreClient replicaClient) {
    Optional<Table> previousTable = replica.getTable(replicaClient, replicaDatabaseName, replicaTableName);
    if (!previousTable.isPresent()) {
      throw new InvalidReplicationModeException("Trying a "
          + ReplicationMode.METADATA_UPDATE.name()
          + " on a table that wasn't replicated before. This is not possible, rerun with a different table name or change the replication mode to "
          + ReplicationMode.FULL.name()
          + ".");

    }
    return previousTable.get().getSd().getLocation();
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
