/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.circustrain.core.metastore.HiveEntityFactory.newFieldSchema;
import static com.hotels.bdp.circustrain.core.metastore.HiveEntityFactory.newPartition;
import static com.hotels.bdp.circustrain.core.metastore.HiveEntityFactory.newStorageDescriptor;
import static com.hotels.bdp.circustrain.core.metastore.HiveEntityFactory.newTable;

import java.io.File;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.ReplicaLocationManager;
import com.hotels.bdp.circustrain.core.replica.InvalidReplicationModeException;
import com.hotels.bdp.circustrain.core.replica.MetadataUpdateReplicaLocationManager;
import com.hotels.bdp.circustrain.core.replica.Replica;
import com.hotels.bdp.circustrain.core.source.Source;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@RunWith(MockitoJUnitRunner.class)
public class PartitionedTableMetadataUpdateReplicationTest {

  public @Rule ExpectedException expectedException = ExpectedException.none();

  private static final short MAX_PARTITIONS = 1;
  private static final String PARTITION_PREDICATE = "partitionPredicate";
  private static final String EVENT_ID = "event_id";
  private static final String TABLE = "table";
  private static final String DATABASE = "database";

  private @Mock Source source;
  private @Mock TableAndStatistics sourceTableAndStatistics;
  private @Mock PartitionsAndStatistics partitionsAndStatistics;
  private @Mock Replica replica;
  private @Mock EventIdFactory eventIdFactory;
  private @Mock PartitionPredicate partitionPredicate;
  private @Mock Supplier<CloseableMetaStoreClient> metastoreClientSupplier;
  private @Mock CloseableMetaStoreClient replicaClient;
  private @Mock Table previousReplicaTable;
  private @Mock StorageDescriptor sd;

  private final String tableLocation = "/tmp/table/location";
  private final String replicaLocation = "/tmp/replica/table/location";
  private final List<FieldSchema> partitionKeys = Lists.newArrayList(newFieldSchema("a"));
  private final Table sourceTable = newTable(TABLE, DATABASE, partitionKeys,
      newStorageDescriptor(new File("bla"), "col1"));
  private final Partition sourcePartition1 = newPartition(sourceTable, "1");
  private final Partition sourcePartition2 = newPartition(sourceTable, "2");
  private final List<Partition> sourcePartitions = Lists.newArrayList(sourcePartition1, sourcePartition2);

  @Before
  public void injectMocks() throws Exception {
    when(eventIdFactory.newEventId(anyString())).thenReturn(EVENT_ID);
    when(source.getTableAndStatistics(DATABASE, TABLE)).thenReturn(sourceTableAndStatistics);
    when(sourceTableAndStatistics.getTable()).thenReturn(sourceTable);
    when(partitionsAndStatistics.getPartitions()).thenReturn(sourcePartitions);
    when(partitionPredicate.getPartitionPredicate()).thenReturn(PARTITION_PREDICATE);
    when(partitionPredicate.getPartitionPredicateLimit()).thenReturn(MAX_PARTITIONS);
    when(replica.getMetaStoreClientSupplier()).thenReturn(metastoreClientSupplier);
    when(metastoreClientSupplier.get()).thenReturn(replicaClient);
  }

  @Test
  public void typical() throws Exception {
    when(source.getPartitions(sourceTable, PARTITION_PREDICATE, MAX_PARTITIONS)).thenReturn(partitionsAndStatistics);
    when(replica.getTable(replicaClient, DATABASE, TABLE)).thenReturn(Optional.of(previousReplicaTable));

    when(previousReplicaTable.getSd()).thenReturn(sd);
    when(sd.getLocation()).thenReturn(tableLocation);

    PartitionedTableMetadataUpdateReplication replication = new PartitionedTableMetadataUpdateReplication(DATABASE,
        TABLE, partitionPredicate, source, replica, eventIdFactory, replicaLocation, DATABASE, TABLE);
    replication.replicate();

    InOrder replicationOrder = inOrder(replica);
    replicationOrder.verify(replica).validateReplicaTable(DATABASE, TABLE);
    replicationOrder
        .verify(replica)
        .updateMetadata(eq(EVENT_ID), eq(sourceTableAndStatistics), any(PartitionsAndStatistics.class), eq(DATABASE),
            eq(TABLE), any(ReplicaLocationManager.class));
  }

  @Test
  public void nonExistingPartitionsAreFiltered() throws Exception {
    when(source.getPartitions(sourceTable, PARTITION_PREDICATE, MAX_PARTITIONS)).thenReturn(partitionsAndStatistics);
    when(replica.getTable(replicaClient, DATABASE, TABLE)).thenReturn(Optional.of(previousReplicaTable));
    when(previousReplicaTable.getSd()).thenReturn(sd);
    when(sd.getLocation()).thenReturn(tableLocation);
    // mimics that the first partitions exist but second partition didn't so it will be filtered
    when(replicaClient.getPartition(DATABASE, TABLE, sourcePartition2.getValues()))
        .thenThrow(new NoSuchObjectException());

    PartitionedTableMetadataUpdateReplication replication = new PartitionedTableMetadataUpdateReplication(DATABASE,
        TABLE, partitionPredicate, source, replica, eventIdFactory, replicaLocation, DATABASE, TABLE);
    replication.replicate();

    InOrder replicationOrder = inOrder(replica);
    replicationOrder.verify(replica).validateReplicaTable(DATABASE, TABLE);
    ArgumentCaptor<PartitionsAndStatistics> partitionsAndStatisticsCaptor = ArgumentCaptor
        .forClass(PartitionsAndStatistics.class);
    replicationOrder
        .verify(replica)
        .updateMetadata(eq(EVENT_ID), eq(sourceTableAndStatistics), partitionsAndStatisticsCaptor.capture(),
            eq(DATABASE), eq(TABLE), any(ReplicaLocationManager.class));
    PartitionsAndStatistics value = partitionsAndStatisticsCaptor.getValue();
    assertThat(value.getPartitions().size(), is(1));
    assertThat(value.getPartitionNames().get(0), is("a=1"));
  }

  @Test
  public void noMatchingPartitions() throws Exception {
    PartitionsAndStatistics emptyPartitionsAndStats = new PartitionsAndStatistics(sourceTable.getPartitionKeys(),
        Collections.<Partition>emptyList(), Collections.<String, List<ColumnStatisticsObj>>emptyMap());
    when(source.getPartitions(sourceTable, PARTITION_PREDICATE, MAX_PARTITIONS)).thenReturn(emptyPartitionsAndStats);

    PartitionedTableMetadataUpdateReplication replication = new PartitionedTableMetadataUpdateReplication(DATABASE,
        TABLE, partitionPredicate, source, replica, eventIdFactory, replicaLocation, DATABASE, TABLE);
    replication.replicate();

    verify(replica).validateReplicaTable(DATABASE, TABLE);
    verify(replica)
        .updateMetadata(eq(EVENT_ID), eq(sourceTableAndStatistics), eq(DATABASE), eq(TABLE),
            any(MetadataUpdateReplicaLocationManager.class));
  }

  @Test
  public void throwExceptionWhenReplicaTableDoesNotExist() throws Exception {
    expectedException.expect(CircusTrainException.class);
    expectedException.expectCause(isA(InvalidReplicationModeException.class));

    when(source.getPartitions(sourceTable, PARTITION_PREDICATE, MAX_PARTITIONS)).thenReturn(partitionsAndStatistics);
    when(replica.getTable(replicaClient, DATABASE, TABLE)).thenReturn(Optional.<Table>absent());

    PartitionedTableMetadataUpdateReplication replication = new PartitionedTableMetadataUpdateReplication(DATABASE,
        TABLE, partitionPredicate, source, replica, eventIdFactory, replicaLocation, DATABASE, TABLE);
    replication.replicate();
  }
}
