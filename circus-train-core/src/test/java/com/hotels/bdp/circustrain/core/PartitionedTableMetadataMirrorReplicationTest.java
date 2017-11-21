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
package com.hotels.bdp.circustrain.core;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.ReplicaLocationManager;
import com.hotels.bdp.circustrain.api.SourceLocationManager;
import com.hotels.bdp.circustrain.core.replica.Replica;
import com.hotels.bdp.circustrain.core.source.Source;

@RunWith(MockitoJUnitRunner.class)
public class PartitionedTableMetadataMirrorReplicationTest {

  private static final short MAX_PARTITIONS = 1;
  private static final String PARTITION_PREDICATE = "partitionPredicate";
  private static final String EVENT_ID = "event_id";
  private static final String TABLE = "table";
  private static final String DATABASE = "database";

  private @Mock Source source;
  private @Mock TableAndStatistics sourceTableAndStatistics;
  private @Mock Table sourceTable;
  private @Mock PartitionsAndStatistics partitionsAndStatistics;
  private @Mock Replica replica;
  private @Mock EventIdFactory eventIdFactory;
  private @Mock SourceLocationManager sourceLocationManager;
  private @Mock PartitionPredicate partitionPredicate;

  private final Partition partition1 = new Partition();
  private final Partition partition2 = new Partition();;
  private final List<Partition> sourcePartitions = Arrays.asList(partition1, partition2);
  private final Map<String, Object> copierOptions = Collections.<String, Object> emptyMap();

  @Before
  public void injectMocks() throws Exception {
    when(eventIdFactory.newEventId(anyString())).thenReturn(EVENT_ID);
    when(source.getTableAndStatistics(DATABASE, TABLE)).thenReturn(sourceTableAndStatistics);
    when(sourceTableAndStatistics.getTable()).thenReturn(sourceTable);
    when(source.getLocationManager(sourceTable, sourcePartitions, EVENT_ID, copierOptions))
        .thenReturn(sourceLocationManager);
    when(partitionsAndStatistics.getPartitions()).thenReturn(sourcePartitions);
    when(partitionPredicate.getPartitionPredicate()).thenReturn(PARTITION_PREDICATE);
    when(partitionPredicate.getPartitionPredicateLimit()).thenReturn(MAX_PARTITIONS);
  }

  @Test
  public void typical() throws Exception {
    when(source.getPartitions(sourceTable, PARTITION_PREDICATE, MAX_PARTITIONS)).thenReturn(partitionsAndStatistics);

    PartitionedTableMetadataMirrorReplication replication = new PartitionedTableMetadataMirrorReplication(DATABASE,
        TABLE, partitionPredicate, source, replica, eventIdFactory, DATABASE, TABLE);
    replication.replicate();

    InOrder replicationOrder = inOrder(sourceLocationManager, replica);
    replicationOrder.verify(replica).validateReplicaTable(DATABASE, TABLE);
    replicationOrder.verify(sourceLocationManager).cleanUpLocations();
    replicationOrder.verify(replica).updateMetadata(eq(EVENT_ID), eq(sourceTableAndStatistics),
        eq(partitionsAndStatistics), eq(sourceLocationManager), eq(DATABASE), eq(TABLE),
        any(ReplicaLocationManager.class));
  }

  @Test
  public void noMatchingPartitions() throws Exception {
    PartitionsAndStatistics emptyPartitionsAndStats = new PartitionsAndStatistics(sourceTable.getPartitionKeys(),
        Collections.<Partition> emptyList(), Collections.<String, List<ColumnStatisticsObj>> emptyMap());
    when(source.getPartitions(sourceTable, PARTITION_PREDICATE, MAX_PARTITIONS)).thenReturn(emptyPartitionsAndStats);

    PartitionedTableMetadataMirrorReplication replication = new PartitionedTableMetadataMirrorReplication(DATABASE,
        TABLE, partitionPredicate, source, replica, eventIdFactory, DATABASE, TABLE);
    replication.replicate();

    verifyZeroInteractions(replica);
  }
}
