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
package com.hotels.bdp.circustrain.core.replica;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyShort;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.REPLICATION_EVENT;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

import com.hotels.bdp.circustrain.api.CircusTrainTableParameter;
import com.hotels.bdp.circustrain.api.conf.ReplicaTable;
import com.hotels.bdp.circustrain.api.conf.SourceTable;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@RunWith(MockitoJUnitRunner.class)
public class DestructiveReplicaTest {

  private static final String DATABASE = "db";
  private static final String TABLE = "table1";
  private static final String REPLICA_TABLE = "table2";
  private static final String EVENT_ID = "eventId";

  private @Mock Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier;
  private @Mock CloseableMetaStoreClient client;
  private @Mock CleanupLocationManager cleanupLocationManager;

  private final TableReplication tableReplication = new TableReplication();
  private DestructiveReplica replica;
  private Table table;
  private final Path tableLocation = new Path("tableLocation");

  @Before
  public void setUp() {
    SourceTable sourceTable = new SourceTable();
    sourceTable.setDatabaseName(DATABASE);
    sourceTable.setTableName(TABLE);
    tableReplication.setSourceTable(sourceTable);
    ReplicaTable replicaTable = new ReplicaTable();
    replicaTable.setDatabaseName(DATABASE);
    replicaTable.setTableName(REPLICA_TABLE);
    tableReplication.setReplicaTable(replicaTable);
    when(replicaMetaStoreClientSupplier.get()).thenReturn(client);
    replica = new DestructiveReplica(replicaMetaStoreClientSupplier, cleanupLocationManager, tableReplication);

    table = new Table();
    table.setDbName(DATABASE);
    table.setTableName(REPLICA_TABLE);
    table.setPartitionKeys(Lists.newArrayList(new FieldSchema("part1", "string", "")));
    Map<String, String> parameters = new HashMap<>();
    parameters.put(CircusTrainTableParameter.SOURCE_TABLE.parameterName(), DATABASE + "." + TABLE);
    parameters.put(REPLICATION_EVENT.parameterName(), EVENT_ID);
    table.setParameters(parameters);
    StorageDescriptor sd1 = new StorageDescriptor();
    sd1.setLocation(tableLocation.toString());
    table.setSd(sd1);
  }

  @Test
  public void tableIsUnderCircusTrainControl() throws Exception {
    when(client.tableExists(DATABASE, REPLICA_TABLE)).thenReturn(true);
    when(client.getTable(DATABASE, REPLICA_TABLE)).thenReturn(table);

    assertThat(replica.tableIsUnderCircusTrainControl(), is(true));
    verify(client).close();
  }

  @Test
  public void tableIsUnderCircusTrainControlTableDoesNotExist() throws Exception {
    when(client.tableExists(DATABASE, REPLICA_TABLE)).thenReturn(false);

    assertThat(replica.tableIsUnderCircusTrainControl(), is(true));
    verify(client).close();
  }

  @Test
  public void tableIsUnderCircusTrainControlParameterDoesNotMatch() throws Exception {
    when(client.tableExists(DATABASE, REPLICA_TABLE)).thenReturn(true);
    table.putToParameters(CircusTrainTableParameter.SOURCE_TABLE.parameterName(), "different.table");
    when(client.getTable(DATABASE, REPLICA_TABLE)).thenReturn(table);

    assertThat(replica.tableIsUnderCircusTrainControl(), is(false));
    verify(client).close();
  }

  @Test
  public void tableIsUnderCircusTrainControlParameterIsNull() throws Exception {
    when(client.tableExists(DATABASE, REPLICA_TABLE)).thenReturn(true);
    Map<String, String> parameters = new HashMap<>();
    table.setParameters(parameters);
    when(client.getTable(DATABASE, REPLICA_TABLE)).thenReturn(table);

    assertThat(replica.tableIsUnderCircusTrainControl(), is(false));
    verify(client).close();
  }

  @Test
  public void dropDeletedPartitions() throws Exception {
    when(client.tableExists(DATABASE, REPLICA_TABLE)).thenReturn(true);
    when(client.getTable(DATABASE, REPLICA_TABLE)).thenReturn(table);
    Path location1 = new Path("loc1");
    Partition replicaPartition1 = newPartition("value1", location1);
    Path location2 = new Path("loc2");
    Partition replicaPartition2 = newPartition("value2", location2);

    List<Partition> replicaPartitions = Lists.newArrayList(replicaPartition1, replicaPartition2);
    mockPartitionIterator(replicaPartitions);

    // No sourcePartitionsNames so dropping all replica partitions
    List<String> sourcePartitionNames = Lists.newArrayList();
    replica.dropDeletedPartitions(sourcePartitionNames);
    verify(client).dropPartition(DATABASE, REPLICA_TABLE, "part1=value1", false);
    verify(client).dropPartition(DATABASE, REPLICA_TABLE, "part1=value2", false);
    verify(cleanupLocationManager).addCleanupLocation(EVENT_ID, location1);
    verify(cleanupLocationManager).addCleanupLocation(EVENT_ID, location2);
    verify(client).close();
    verify(cleanupLocationManager).scheduleLocations();
  }

  @Test
  public void dropDeletedPartitionsTableDoesNotExist() throws Exception {
    when(client.tableExists(DATABASE, REPLICA_TABLE)).thenReturn(false);

    replica.dropDeletedPartitions(Lists.<String> newArrayList());
    verify(client, never()).dropPartition(eq(DATABASE), eq(REPLICA_TABLE), anyString(), anyBoolean());
    verify(cleanupLocationManager, never()).addCleanupLocation(anyString(), any(Path.class));
    verify(client).close();
    verify(cleanupLocationManager).scheduleLocations();
  }

  @Test
  public void dropDeletedPartitionsNothingToDrop() throws Exception {
    when(client.getTable(DATABASE, REPLICA_TABLE)).thenReturn(table);
    Partition replicaPartition = new Partition();
    replicaPartition.setValues(Lists.newArrayList("value1"));

    List<Partition> replicaPartitions = Lists.newArrayList(replicaPartition);
    mockPartitionIterator(replicaPartitions);

    // source partition ("part1=value1") is the same as target partition nothing is deleted on source, nothing should be
    // deleted on target
    List<String> sourcePartitionNames = Lists.newArrayList("part1=value1");
    replica.dropDeletedPartitions(sourcePartitionNames);
    verify(client, never()).dropPartition(DATABASE, REPLICA_TABLE, "part1=value1", false);
    verify(cleanupLocationManager, never()).addCleanupLocation(anyString(), any(Path.class));
    verify(client).close();
    verify(cleanupLocationManager).scheduleLocations();
  }

  @Test
  public void dropDeletedPartitionsUnpartitionedTable() throws Exception {
    table.setPartitionKeys(null);
    when(client.getTable(DATABASE, REPLICA_TABLE)).thenReturn(table);

    List<String> sourcePartitionNames = Lists.newArrayList();
    replica.dropDeletedPartitions(sourcePartitionNames);
    verify(client, never()).dropPartition(eq(DATABASE), eq(REPLICA_TABLE), anyString(), eq(false));
    verify(cleanupLocationManager, never()).addCleanupLocation(anyString(), any(Path.class));
    verify(client).close();
    verify(cleanupLocationManager).scheduleLocations();
  }

  @Test
  public void dropTablePartitioned() throws Exception {
    when(client.tableExists(DATABASE, REPLICA_TABLE)).thenReturn(true);
    when(client.getTable(DATABASE, REPLICA_TABLE)).thenReturn(table);
    Path location1 = new Path("loc1");
    Partition replicaPartition1 = newPartition("value1", location1);
    Path location2 = new Path("loc2");
    Partition replicaPartition2 = newPartition("value2", location2);

    List<Partition> replicaPartitions = Lists.newArrayList(replicaPartition1, replicaPartition2);
    mockPartitionIterator(replicaPartitions);

    replica.dropTable();
    verify(client).dropPartition(DATABASE, REPLICA_TABLE, "part1=value1", false);
    verify(client).dropPartition(DATABASE, REPLICA_TABLE, "part1=value2", false);
    verify(client).dropTable(DATABASE, REPLICA_TABLE, false, true);
    verify(cleanupLocationManager).addCleanupLocation(EVENT_ID, location1);
    verify(cleanupLocationManager).addCleanupLocation(EVENT_ID, location2);
    verify(cleanupLocationManager).addCleanupLocation(EVENT_ID, tableLocation);
    verify(client).close();
    verify(cleanupLocationManager).scheduleLocations();
  }

  @Test
  public void dropTableButTableDoesNotExist() throws Exception {
    when(client.tableExists(DATABASE, REPLICA_TABLE)).thenReturn(false);
    List<Partition> replicaPartitions = Lists.newArrayList();
    mockPartitionIterator(replicaPartitions);

    replica.dropTable();
    verify(client, never()).dropPartition(eq(DATABASE), eq(REPLICA_TABLE), anyString(), anyBoolean());
    verify(client, never()).dropTable(eq(DATABASE), eq(REPLICA_TABLE), anyBoolean(), anyBoolean());
    verify(cleanupLocationManager, never()).addCleanupLocation(anyString(), any(Path.class));
    verify(client).close();
    verify(cleanupLocationManager).scheduleLocations();
  }

  @Test
  public void dropTableUnpartitioned() throws Exception {
    when(client.tableExists(DATABASE, REPLICA_TABLE)).thenReturn(true);
    table.setPartitionKeys(null);
    when(client.getTable(DATABASE, REPLICA_TABLE)).thenReturn(table);

    replica.dropTable();
    verify(client).dropTable(DATABASE, REPLICA_TABLE, false, true);
    verify(cleanupLocationManager).addCleanupLocation(EVENT_ID, tableLocation);
    verify(client).close();
    verify(cleanupLocationManager).scheduleLocations();
  }

  private void mockPartitionIterator(List<Partition> replicaPartitions)
    throws MetaException, TException, NoSuchObjectException {
    List<String> replicaPartitionNames = Lists.newArrayList("value1");
    when(client.listPartitionNames(eq(DATABASE), eq(REPLICA_TABLE), anyShort())).thenReturn(replicaPartitionNames);
    when(client.getPartitionsByNames(DATABASE, REPLICA_TABLE, replicaPartitionNames)).thenReturn(replicaPartitions);
  }

  private Partition newPartition(String partitionValue, Path location1) {
    Partition partition = new Partition();
    partition.setValues(Lists.newArrayList(partitionValue));
    StorageDescriptor sd1 = new StorageDescriptor();
    sd1.setLocation(location1.toString());
    partition.setSd(sd1);
    Map<String, String> parameters = new HashMap<>();
    parameters.put(REPLICATION_EVENT.parameterName(), EVENT_ID);
    partition.setParameters(parameters);
    return partition;
  }

}
