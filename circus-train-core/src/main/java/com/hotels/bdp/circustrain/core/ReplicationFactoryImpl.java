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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.Replication;
import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.bdp.circustrain.api.conf.SourceTable;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.copier.CopierFactoryManager;
import com.hotels.bdp.circustrain.api.copier.CopierOptions;
import com.hotels.bdp.circustrain.api.data.DataManipulatorFactoryManager;
import com.hotels.bdp.circustrain.api.event.CopierListener;
import com.hotels.bdp.circustrain.core.replica.Replica;
import com.hotels.bdp.circustrain.core.replica.ReplicaFactory;
import com.hotels.bdp.circustrain.core.source.Source;
import com.hotels.bdp.circustrain.core.source.SourceFactory;

public class ReplicationFactoryImpl implements ReplicationFactory {

  private final EventIdFactory eventIdFactory = EventIdFactory.DEFAULT;
  private final SourceFactory sourceFactory;
  private final ReplicaFactory replicaFactory;
  private final CopierFactoryManager copierFactoryManager;
  private final CopierListener copierListener;
  private final PartitionPredicateFactory partitionPredicateFactory;
  private final CopierOptions copierOptions;
  private final DataManipulatorFactoryManager dataManipulatorFactoryManager;

  public ReplicationFactoryImpl(
      SourceFactory sourceFactory,
      ReplicaFactory replicaFactory,
      CopierFactoryManager copierFactoryManager,
      CopierListener copierListener,
      PartitionPredicateFactory partitionPredicateFactory,
      CopierOptions copierOptions,
      DataManipulatorFactoryManager dataManipulatorFactoryManager) {
    this.sourceFactory = sourceFactory;
    this.replicaFactory = replicaFactory;
    this.copierFactoryManager = copierFactoryManager;
    this.copierListener = copierListener;
    this.partitionPredicateFactory = partitionPredicateFactory;
    this.copierOptions = copierOptions;
    this.dataManipulatorFactoryManager = dataManipulatorFactoryManager;
  }

  /*
   * (non-Javadoc)
   * @see com.hotels.bdp.circustrain.core.ReplicationFactory#newInstance(com.hotels.bdp.circustrain.core.conf.
   * TableReplication)
   */
  @Override
  public Replication newInstance(TableReplication tableReplication) {
    Replica replica = replicaFactory.newInstance(tableReplication);
    SourceTable sourceTable = tableReplication.getSourceTable();
    String sourceDatabaseName = sourceTable.getDatabaseName();
    String sourceTableName = sourceTable.getTableName();

    Source source = sourceFactory.newInstance(tableReplication);
    validate(tableReplication, source, replica);
    TableAndStatistics tableAndStatistics = source.getTableAndStatistics(sourceDatabaseName, sourceTableName);
    List<FieldSchema> partitionKeys = tableAndStatistics.getTable().getPartitionKeys();

    Replication replication = null;
    if (partitionKeys == null || partitionKeys.isEmpty()) {
      replication = createUnpartitionedTableReplication(tableReplication, source, replica);
    } else {
      replication = createPartitionedTableReplication(tableReplication, source, replica);
    }
    return replication;
  }

  private Replication createPartitionedTableReplication(
      TableReplication tableReplication,
      Source source,
      Replica replica) {
    Replication replication = null;
    PartitionPredicate partitionPredicate = partitionPredicateFactory.newInstance(tableReplication);
    switch (tableReplication.getReplicationMode()) {
    case METADATA_MIRROR:
      replication = new PartitionedTableMetadataMirrorReplication(tableReplication.getSourceTable().getDatabaseName(),
          tableReplication.getSourceTable().getTableName(), partitionPredicate, source, replica, eventIdFactory,
          tableReplication.getReplicaDatabaseName(), tableReplication.getReplicaTableName());
      break;
    case FULL_OVERWRITE:
    case FULL:
      Map<String, Object> mergedCopierOptions = tableReplication
          .getMergedCopierOptions(copierOptions.getCopierOptions());
      replication = new PartitionedTableReplication(tableReplication.getSourceTable().getDatabaseName(),
          tableReplication.getSourceTable().getTableName(), partitionPredicate, source, replica, copierFactoryManager,
          eventIdFactory, tableReplication.getReplicaTable().getTableLocation(),
          tableReplication.getReplicaDatabaseName(), tableReplication.getReplicaTableName(), mergedCopierOptions,
          copierListener, dataManipulatorFactoryManager);
      break;
    case METADATA_UPDATE:
      replication = new PartitionedTableMetadataUpdateReplication(tableReplication.getSourceTable().getDatabaseName(),
          tableReplication.getSourceTable().getTableName(), partitionPredicate, source, replica, eventIdFactory,
          tableReplication.getReplicaTable().getTableLocation(), tableReplication.getReplicaDatabaseName(),
          tableReplication.getReplicaTableName());
      break;
    default:
      throw new CircusTrainException(
          String.format("ReplicationMode %s is unsupported.", tableReplication.getReplicationMode()));
    }
    return replication;
  }

  private Replication createUnpartitionedTableReplication(
      TableReplication tableReplication,
      Source source,
      Replica replica) {
    Replication replication = null;
    switch (tableReplication.getReplicationMode()) {
    case METADATA_MIRROR:
      replication = new UnpartitionedTableMetadataMirrorReplication(tableReplication.getSourceTable().getDatabaseName(),
          tableReplication.getSourceTable().getTableName(), source, replica, eventIdFactory,
          tableReplication.getReplicaDatabaseName(), tableReplication.getReplicaTableName());
      break;
    case FULL_OVERWRITE:
    case FULL:
      Map<String, Object> mergedCopierOptions = tableReplication
          .getMergedCopierOptions(copierOptions.getCopierOptions());
      replication = new UnpartitionedTableReplication(tableReplication, source, replica, copierFactoryManager,
          eventIdFactory, mergedCopierOptions, copierListener, dataManipulatorFactoryManager);
      break;
    case METADATA_UPDATE:
      replication = new UnpartitionedTableMetadataUpdateReplication(tableReplication.getSourceTable().getDatabaseName(),
          tableReplication.getSourceTable().getTableName(), source, replica, eventIdFactory,
          tableReplication.getReplicaDatabaseName(), tableReplication.getReplicaTableName());
      break;
    default:
      throw new CircusTrainException(
          String.format("ReplicationMode %s is unsupported.", tableReplication.getReplicationMode()));
    }
    return replication;
  }

  private void validate(TableReplication tableReplication, Source source, Replica replica) {
    source.getDatabase(tableReplication.getSourceTable().getDatabaseName());
    replica.getDatabase(tableReplication.getReplicaDatabaseName());

    TableAndStatistics sourceTableAndStatistics = source.getTableAndStatistics(tableReplication);
    if (tableReplication.getReplicationMode() != ReplicationMode.METADATA_MIRROR
        && MetaStoreUtils.isView(sourceTableAndStatistics.getTable())) {
      throw new CircusTrainException(String
          .format("Cannot replicate view %s. Only %s is supported for views",
              tableReplication.getSourceTable().getQualifiedName(), ReplicationMode.METADATA_MIRROR.name()));
    }
  }

}
