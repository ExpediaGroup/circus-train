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

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.ReplicaLocationManager;
import com.hotels.bdp.circustrain.api.SourceLocationManager;
import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.copier.CopierFactory;
import com.hotels.bdp.circustrain.api.copier.CopierFactoryManager;
import com.hotels.bdp.circustrain.api.data.DataManipulator;
import com.hotels.bdp.circustrain.api.data.DataManipulatorFactory;
import com.hotels.bdp.circustrain.api.data.DataManipulatorFactoryManager;
import com.hotels.bdp.circustrain.api.event.CopierListener;
import com.hotels.bdp.circustrain.api.metrics.Metrics;
import com.hotels.bdp.circustrain.core.replica.Replica;
import com.hotels.bdp.circustrain.core.replica.TableType;
import com.hotels.bdp.circustrain.core.source.Source;

@RunWith(MockitoJUnitRunner.class)
public class PartitionedTableReplicationTest {

  private static final short MAX_PARTITIONS = 1;
  private static final String PARTITION_PREDICATE = "partitionPredicate";
  private static final String EVENT_ID = "event_id";
  private static final String TABLE = "table";
  private static final String DATABASE = "database";
  private static final String MAPPED_TABLE = "mapped_table";
  private static final String MAPPED_DATABASE = "mapped_database";

  private @Mock Source source;
  private @Mock TableAndStatistics sourceTableAndStatistics;
  private @Mock Table sourceTable;
  private @Mock PartitionsAndStatistics partitionsAndStatistics;
  private @Mock Replica replica;
  private @Mock CopierFactoryManager copierFactoryManager;
  private @Mock CopierFactory copierFactory;
  private @Mock Copier copier;
  private @Mock Map<String, Object> copierOptions;
  private @Mock EventIdFactory eventIdFactory;
  private @Mock SourceLocationManager sourceLocationManager;
  private @Mock ReplicaLocationManager replicaLocationManager;
  private @Mock CopierListener listener;
  private @Mock PartitionPredicate partitionPredicate;
  private @Mock DataManipulatorFactoryManager dataManipulatorFactoryManager;
  private @Mock DataManipulatorFactory dataManipulatorFactory;
  private @Mock DataManipulator dataManipulator;

  private final Path sourceTableLocation = new Path("sourceTableLocation");
  private final Path replicaTableLocation = new Path("replicaTableLocation");
  private final Partition partition1 = new Partition();
  private final Partition partition2 = new Partition();;
  private final List<Partition> sourcePartitions = Arrays.asList(partition1, partition2);
  private final List<Path> sourcePartitionLocations = Arrays.asList(new Path("partition1"), new Path("partition2"));
  private final String targetTableLocation = "s3a:/targetTableLocation";

  @Before
  public void injectMocks() throws Exception {
    when(eventIdFactory.newEventId(anyString())).thenReturn(EVENT_ID);
    when(source.getTableAndStatistics(DATABASE, TABLE)).thenReturn(sourceTableAndStatistics);
    when(sourceTableAndStatistics.getTable()).thenReturn(sourceTable);
    when(source.getLocationManager(sourceTable, sourcePartitions, EVENT_ID, copierOptions))
        .thenReturn(sourceLocationManager);
    when(sourceLocationManager.getTableLocation()).thenReturn(sourceTableLocation);
    when(sourceLocationManager.getPartitionLocations()).thenReturn(sourcePartitionLocations);
    when(replicaLocationManager.getPartitionBaseLocation()).thenReturn(replicaTableLocation);
    when(copierFactoryManager.getCopierFactory(sourceTableLocation, replicaTableLocation, copierOptions))
        .thenReturn(copierFactory);
    when(copierFactory
        .newInstance(EVENT_ID, sourceTableLocation, sourcePartitionLocations, replicaTableLocation, copierOptions))
            .thenReturn(copier);
    when(partitionsAndStatistics.getPartitions()).thenReturn(sourcePartitions);
    when(copierOptions.get("task-count")).thenReturn(Integer.valueOf(2));
    when(partitionPredicate.getPartitionPredicate()).thenReturn(PARTITION_PREDICATE);
    when(partitionPredicate.getPartitionPredicateLimit()).thenReturn(MAX_PARTITIONS);
    when(dataManipulatorFactoryManager.getFactory(sourceTableLocation, replicaTableLocation, copierOptions))
        .thenReturn(dataManipulatorFactory);
    when(dataManipulatorFactory.newInstance(replicaTableLocation, copierOptions)).thenReturn(dataManipulator);
  }

  @Test
  public void noMatchingPartitions() throws Exception {
    when(replica.getLocationManager(TableType.PARTITIONED, targetTableLocation, EVENT_ID, sourceLocationManager))
        .thenReturn(replicaLocationManager);
    PartitionsAndStatistics emptyPartitionsAndStats = new PartitionsAndStatistics(sourceTable.getPartitionKeys(),
        Collections.<Partition>emptyList(), Collections.<String, List<ColumnStatisticsObj>>emptyMap());
    when(source.getPartitions(sourceTable, PARTITION_PREDICATE, MAX_PARTITIONS)).thenReturn(emptyPartitionsAndStats);
    when(source.getLocationManager(sourceTable, Collections.<Partition>emptyList(), EVENT_ID, copierOptions))
        .thenReturn(sourceLocationManager);

    PartitionedTableReplication replication = new PartitionedTableReplication(DATABASE, TABLE, partitionPredicate,
        source, replica, copierFactoryManager, eventIdFactory, targetTableLocation, DATABASE, TABLE, copierOptions,
        listener, dataManipulatorFactoryManager);
    replication.replicate();

    verifyZeroInteractions(copier);
    InOrder replicationOrder = inOrder(sourceLocationManager, replica, replicaLocationManager, listener);
    replicationOrder.verify(replica).validateReplicaTable(DATABASE, TABLE);
    replicationOrder
        .verify(replica)
        .updateMetadata(EVENT_ID, sourceTableAndStatistics, DATABASE, TABLE, replicaLocationManager);
  }

  @Test
  public void typical() throws Exception {
    when(replica.getLocationManager(TableType.PARTITIONED, targetTableLocation, EVENT_ID, sourceLocationManager))
        .thenReturn(replicaLocationManager);
    when(source.getPartitions(sourceTable, PARTITION_PREDICATE, MAX_PARTITIONS)).thenReturn(partitionsAndStatistics);

    PartitionedTableReplication replication = new PartitionedTableReplication(DATABASE, TABLE, partitionPredicate,
        source, replica, copierFactoryManager, eventIdFactory, targetTableLocation, DATABASE, TABLE, copierOptions,
        listener, dataManipulatorFactoryManager);
    replication.replicate();

    InOrder replicationOrder = inOrder(copierFactoryManager, copierFactory, copier, sourceLocationManager, replica,
        replicaLocationManager, listener);
    replicationOrder.verify(replica).validateReplicaTable(DATABASE, TABLE);
    replicationOrder
        .verify(copierFactoryManager)
        .getCopierFactory(sourceTableLocation, replicaTableLocation, copierOptions);
    replicationOrder
        .verify(copierFactory)
        .newInstance(EVENT_ID, sourceTableLocation, sourcePartitionLocations, replicaTableLocation, copierOptions);
    replicationOrder.verify(listener).copierStart(anyString());
    replicationOrder.verify(copier).copy();
    replicationOrder.verify(listener).copierEnd(any(Metrics.class));
    replicationOrder.verify(sourceLocationManager).cleanUpLocations();
    replicationOrder
        .verify(replica)
        .updateMetadata(EVENT_ID, sourceTableAndStatistics, partitionsAndStatistics, DATABASE, TABLE,
            replicaLocationManager);
    replicationOrder.verify(replicaLocationManager).cleanUpLocations();
  }

  @Test
  public void mappedNames() throws Exception {
    when(replica.getLocationManager(TableType.PARTITIONED, targetTableLocation, EVENT_ID, sourceLocationManager))
        .thenReturn(replicaLocationManager);
    when(source.getPartitions(sourceTable, PARTITION_PREDICATE, MAX_PARTITIONS)).thenReturn(partitionsAndStatistics);

    PartitionedTableReplication replication = new PartitionedTableReplication(DATABASE, TABLE, partitionPredicate,
        source, replica, copierFactoryManager, eventIdFactory, targetTableLocation, MAPPED_DATABASE, MAPPED_TABLE,
        copierOptions, listener, dataManipulatorFactoryManager);
    replication.replicate();

    InOrder replicationOrder = inOrder(copierFactoryManager, copierFactory, copier, sourceLocationManager, replica,
        replicaLocationManager);
    replicationOrder.verify(replica).validateReplicaTable(MAPPED_DATABASE, MAPPED_TABLE);
    replicationOrder
        .verify(copierFactoryManager)
        .getCopierFactory(sourceTableLocation, replicaTableLocation, copierOptions);
    replicationOrder
        .verify(copierFactory)
        .newInstance(EVENT_ID, sourceTableLocation, sourcePartitionLocations, replicaTableLocation, copierOptions);
    replicationOrder.verify(copier).copy();
    replicationOrder.verify(sourceLocationManager).cleanUpLocations();
    replicationOrder
        .verify(replica)
        .updateMetadata(EVENT_ID, sourceTableAndStatistics, partitionsAndStatistics, MAPPED_DATABASE, MAPPED_TABLE,
            replicaLocationManager);
    replicationOrder.verify(replicaLocationManager).cleanUpLocations();
  }

  @Test
  public void copierListenerCalledWhenException() throws Exception {
    when(replica.getLocationManager(TableType.PARTITIONED, targetTableLocation, EVENT_ID, sourceLocationManager))
        .thenReturn(replicaLocationManager);
    when(copier.copy()).thenThrow(new CircusTrainException("copy failed"));
    when(source.getPartitions(sourceTable, PARTITION_PREDICATE, MAX_PARTITIONS)).thenReturn(partitionsAndStatistics);

    PartitionedTableReplication replication = new PartitionedTableReplication(DATABASE, TABLE, partitionPredicate,
        source, replica, copierFactoryManager, eventIdFactory, targetTableLocation, DATABASE, TABLE, copierOptions,
        listener, dataManipulatorFactoryManager);
    try {
      replication.replicate();
      fail("Copy exception should be caught and rethrown");
    } catch (CircusTrainException e) {
      InOrder replicationOrder = inOrder(copier, listener);
      replicationOrder.verify(listener).copierStart(anyString());
      replicationOrder.verify(copier).copy();
      // Still called
      replicationOrder.verify(listener).copierEnd(any(Metrics.class));
    }
  }

  @Test
  public void replicationFailsOnDeleteTableException() throws Exception {
    when(replica.getLocationManager(TableType.PARTITIONED, targetTableLocation, EVENT_ID, sourceLocationManager))
        .thenReturn(replicaLocationManager);
    when(source.getPartitions(sourceTable, PARTITION_PREDICATE, MAX_PARTITIONS)).thenReturn(partitionsAndStatistics);
    doThrow(new Exception()).when(replica).cleanupReplicaTableIfRequired(DATABASE, TABLE, dataManipulator);

    PartitionedTableReplication replication = new PartitionedTableReplication(DATABASE, TABLE, partitionPredicate,
        source, replica, copierFactoryManager, eventIdFactory, targetTableLocation, DATABASE, TABLE, copierOptions,
        listener, dataManipulatorFactoryManager);
    try {
      replication.replicate();
      fail("Copy exception should be caught and rethrown");
    } catch (CircusTrainException e) {
      InOrder replicationOrder = inOrder(copier, listener);
      replicationOrder.verify(listener).copierStart(anyString());
      replicationOrder.verify(copier).copy();
      // Still called
      replicationOrder.verify(listener).copierEnd(any(Metrics.class));
    }
  }
}
