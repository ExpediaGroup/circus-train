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
package com.hotels.bdp.circustrain.core.replica;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.REPLICATION_EVENT;
import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.REPLICATION_MODE;
import static com.hotels.bdp.circustrain.api.conf.ReplicationMode.FULL;
import static com.hotels.bdp.circustrain.api.conf.ReplicationMode.FULL_OVERWRITE;
import static com.hotels.bdp.circustrain.api.conf.ReplicationMode.METADATA_MIRROR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.ListUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData._Fields;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.ReplicaLocationManager;
import com.hotels.bdp.circustrain.api.conf.ReplicaCatalog;
import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.event.ReplicaCatalogListener;
import com.hotels.bdp.circustrain.api.listener.HousekeepingListener;
import com.hotels.bdp.circustrain.api.metadata.ColumnStatisticsTransformation;
import com.hotels.bdp.circustrain.api.metadata.PartitionTransformation;
import com.hotels.bdp.circustrain.api.metadata.TableTransformation;
import com.hotels.bdp.circustrain.core.PartitionsAndStatistics;
import com.hotels.bdp.circustrain.core.TableAndStatistics;
import com.hotels.bdp.circustrain.core.replica.hive.AlterTableService;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.exception.MetaStoreClientException;

@RunWith(MockitoJUnitRunner.class)
public class ReplicaTest {

  private static final String NAME = "name";
  private static final String TABLE_NAME = "tableName";
  private static final String DB_NAME = "dbName";
  private static final String EVENT_ID = "eventId";
  private static final String SOURCE_META_STORE_URIS = "sourceMetaStoreUris";
  private static final String REPLICA_META_STORE_URIS = "replicaMetaStoreUris";
  private static final String COLUMN_A = "a";
  private static final String COLUMN_B = "b";
  private static final String COLUMN_C = "c";
  private static final String COLUMN_D = "d";
  private static final FieldSchema FIELD_A = new FieldSchema(COLUMN_A, "string", null);
  private static final FieldSchema FIELD_B = new FieldSchema(COLUMN_B, "string", null);
  private static final FieldSchema FIELD_C = new FieldSchema(COLUMN_C, "string", null);
  private static final FieldSchema FIELD_D = new FieldSchema(COLUMN_D, "string", null);
  private static final List<FieldSchema> FIELDS = Arrays.asList(FIELD_A, FIELD_B);
  private static final List<FieldSchema> PARTITIONS = Arrays.asList(FIELD_C, FIELD_D);
  private static final int TEST_PARTITION_BATCH_SIZE = 17;

  public @Rule TemporaryFolder temporaryFolder = new TemporaryFolder();

  private @Mock ReplicaCatalog replicaCatalog;
  private @Mock Supplier<CloseableMetaStoreClient> metaStoreClientSupplier;
  private @Mock CloseableMetaStoreClient mockMetaStoreClient;
  private @Mock ReplicaLocationManager mockReplicaLocationManager;
  private @Captor ArgumentCaptor<List<Partition>> alterPartitionCaptor;
  private @Captor ArgumentCaptor<List<Partition>> addPartitionCaptor;
  private @Captor ArgumentCaptor<SetPartitionsStatsRequest> setStatsRequestCaptor;
  private @Mock HousekeepingListener houseKeepingListener;
  private @Mock ReplicaCatalogListener replicaCatalogListener;
  private @Mock AlterTableService alterTableService;

  private final ReplicaTableFactory tableFactory = new ReplicaTableFactory(SOURCE_META_STORE_URIS,
      TableTransformation.IDENTITY, PartitionTransformation.IDENTITY, ColumnStatisticsTransformation.IDENTITY);
  private Replica replica;
  private TableAndStatistics tableAndStatistics;
  private Table sourceTable;
  private Table existingReplicaTable;
  private List<ColumnStatisticsObj> columnStatisticsObjs;
  private ColumnStatistics columnStatistics;
  private String tableLocation;
  private Partition existingPartition;
  private HiveConf hiveConf;
  private TableReplication tableReplication = new TableReplication();

  @Before
  public void prepare() throws Exception {
    when(metaStoreClientSupplier.get()).thenReturn(mockMetaStoreClient);
    when(replicaCatalog.getName()).thenReturn(NAME);

    hiveConf = new HiveConf();
    hiveConf.setVar(ConfVars.METASTOREURIS, REPLICA_META_STORE_URIS);
    replica = newReplica(tableReplication);
    tableLocation = temporaryFolder.newFolder("table_location").toURI().toString();

    sourceTable = newTable();
    existingPartition = newPartition("one", "two");

    ColumnStatisticsObj columnStatisticsObj1 = new ColumnStatisticsObj(COLUMN_A, "string",
        new ColumnStatisticsData(_Fields.LONG_STATS, new LongColumnStatsData(0, 1)));
    ColumnStatisticsObj columnStatisticsObj2 = new ColumnStatisticsObj(COLUMN_B, "string",
        new ColumnStatisticsData(_Fields.LONG_STATS, new LongColumnStatsData(1, 2)));
    columnStatisticsObjs = Arrays.asList(columnStatisticsObj1, columnStatisticsObj2);
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, DB_NAME, TABLE_NAME);
    columnStatistics = new ColumnStatistics(statsDesc, columnStatisticsObjs);

    tableAndStatistics = new TableAndStatistics(sourceTable, columnStatistics);

    existingReplicaTable = new Table(sourceTable);

    when(mockReplicaLocationManager.getTableLocation()).thenReturn(new Path(tableLocation));
    when(mockReplicaLocationManager.getPartitionBaseLocation()).thenReturn(new Path(tableLocation));

    when(mockMetaStoreClient.getTable(DB_NAME, TABLE_NAME)).thenReturn(existingReplicaTable);
  }

  private void convertSourceTableToView() {
    sourceTable.setTableType(TableType.VIRTUAL_VIEW.name());
    sourceTable.getSd().setLocation(null);
  }

  private void convertExistingReplicaTableToView() {
    when(mockReplicaLocationManager.getTableLocation()).thenReturn(null);
    when(mockReplicaLocationManager.getPartitionBaseLocation()).thenReturn(null);
    existingReplicaTable.setTableType(TableType.VIRTUAL_VIEW.name());
    existingReplicaTable.getSd().setLocation(null);
    existingPartition.getSd().setLocation(null);
  }

  private Replica newReplica(TableReplication tableReplication) {
    return new Replica(replicaCatalog, hiveConf, metaStoreClientSupplier, tableFactory, houseKeepingListener,
        replicaCatalogListener, tableReplication, alterTableService, TEST_PARTITION_BATCH_SIZE);
  }

  @Test
  public void alteringExistingUnpartitionedReplicaTableSucceeds() throws TException, IOException {
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica.updateMetadata(EVENT_ID, tableAndStatistics, DB_NAME, TABLE_NAME, mockReplicaLocationManager);
    verify(alterTableService).alterTable(eq(mockMetaStoreClient), eq(existingReplicaTable), any(Table.class));
    verify(mockMetaStoreClient).updateTableColumnStatistics(columnStatistics);
    verify(mockReplicaLocationManager, never()).addCleanUpLocation(anyString(), any(Path.class));
  }

  @Test
  public void alteringExistingUnpartitionedReplicaTableWithNoStatsSucceeds() throws TException, IOException {
    tableAndStatistics = new TableAndStatistics(sourceTable, null);
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica.updateMetadata(EVENT_ID, tableAndStatistics, DB_NAME, TABLE_NAME, mockReplicaLocationManager);
    verify(alterTableService).alterTable(eq(mockMetaStoreClient), eq(existingReplicaTable), any(Table.class));
    verify(mockMetaStoreClient, never()).updateTableColumnStatistics(any(ColumnStatistics.class));
    verify(mockReplicaLocationManager, never()).addCleanUpLocation(anyString(), any(Path.class));
  }

  @Test
  public void alteringExistingUnpartitionedReplicaViewSucceeds() throws TException, IOException {
    convertSourceTableToView();
    convertExistingReplicaTableToView();
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica.updateMetadata(EVENT_ID, tableAndStatistics, DB_NAME, TABLE_NAME, mockReplicaLocationManager);
    verify(alterTableService).alterTable(eq(mockMetaStoreClient), eq(existingReplicaTable), any(Table.class));
    verify(mockReplicaLocationManager, never()).addCleanUpLocation(anyString(), any(Path.class));
  }

  @Test(expected = CircusTrainException.class)
  public void tryToReplaceExistingUnpartitionedReplicaTableWithView() throws TException, IOException {
    convertSourceTableToView();
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica.updateMetadata(EVENT_ID, tableAndStatistics, DB_NAME, TABLE_NAME, mockReplicaLocationManager);
  }

  @Test(expected = CircusTrainException.class)
  public void tryToReplaceExistingUnpartitionedReplicaViewWithTable() throws TException, IOException {
    convertExistingReplicaTableToView();
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica.updateMetadata(EVENT_ID, tableAndStatistics, DB_NAME, TABLE_NAME, mockReplicaLocationManager);
  }

  @Test(expected = DestinationNotReplicaException.class)
  public void validateReplicaTableOnNonReplicicatedTableFails() throws TException, IOException {
    try {
      replica.validateReplicaTable(DB_NAME, TABLE_NAME);
    } catch (DestinationNotReplicaException e) {
      // Check that nothing was written to the metastore
      verify(mockMetaStoreClient).getTable(DB_NAME, TABLE_NAME);
      verify(mockMetaStoreClient).close();
      verifyNoMoreInteractions(mockMetaStoreClient);
      throw e;
    }
  }

  @Test
  public void validateReplicaTableOnAMirroredTableFails() throws TException, IOException {
    try {
      existingReplicaTable.putToParameters(REPLICATION_EVENT.parameterName(), "previousEventId");
      existingReplicaTable.putToParameters(REPLICATION_MODE.parameterName(), ReplicationMode.METADATA_MIRROR.name());
      replica.validateReplicaTable(DB_NAME, TABLE_NAME);
      fail("Should have thrown InvalidReplicationModeException");
    } catch (InvalidReplicationModeException e) {
      // Check that nothing was written to the metastore
      verify(mockMetaStoreClient).getTable(DB_NAME, TABLE_NAME);
      verify(mockMetaStoreClient).close();
      verifyNoMoreInteractions(mockMetaStoreClient);
    }
  }

  @Test
  public void validateMetadataUpdateReplicaTableOnAMirroredTableFails() throws TException, IOException {
    try {
      existingReplicaTable.putToParameters(REPLICATION_EVENT.parameterName(), "previousEventId");
      existingReplicaTable.putToParameters(REPLICATION_MODE.parameterName(), ReplicationMode.METADATA_MIRROR.name());
      tableReplication.setReplicationMode(ReplicationMode.METADATA_UPDATE);
      replica = newReplica(tableReplication);
      replica.validateReplicaTable(DB_NAME, TABLE_NAME);
      fail("Should have thrown InvalidReplicationModeException");
    } catch (InvalidReplicationModeException e) {
      // Check that nothing was written to the metastore
      verify(mockMetaStoreClient).getTable(DB_NAME, TABLE_NAME);
      verify(mockMetaStoreClient).close();
      verifyNoMoreInteractions(mockMetaStoreClient);
    }
  }

  @Test
  public void validateReplicaTableMetadataMirrorOnExistingNoReplicationModeSetTableFails()
    throws TException, IOException {
    try {
      existingReplicaTable.putToParameters(REPLICATION_EVENT.parameterName(), "previousEventId");
      // no replicationMode set in existing table
      tableReplication.setReplicationMode(ReplicationMode.METADATA_MIRROR);
      replica = newReplica(tableReplication);
      replica.validateReplicaTable(DB_NAME, TABLE_NAME);
      fail("Should have thrown InvalidReplicationModeException");
    } catch (InvalidReplicationModeException e) {
      // Check that nothing was written to the metastore
      verify(mockMetaStoreClient).getTable(DB_NAME, TABLE_NAME);
      verify(mockMetaStoreClient).close();
      verifyNoMoreInteractions(mockMetaStoreClient);
    }
  }

  @Test
  public void validateReplicaTableMetadataMirrorOnExistingFullReplicationTableFails() throws TException, IOException {
    try {
      existingReplicaTable.putToParameters(REPLICATION_EVENT.parameterName(), "previousEventId");
      existingReplicaTable.putToParameters(REPLICATION_MODE.parameterName(), ReplicationMode.FULL.name());
      tableReplication.setReplicationMode(ReplicationMode.METADATA_MIRROR);
      replica = newReplica(tableReplication);
      replica.validateReplicaTable(DB_NAME, TABLE_NAME);
      fail("Should have thrown InvalidReplicationModeException");
    } catch (InvalidReplicationModeException e) {
      // Check that nothing was written to the metastore
      verify(mockMetaStoreClient).getTable(DB_NAME, TABLE_NAME);
      verify(mockMetaStoreClient).close();
      verifyNoMoreInteractions(mockMetaStoreClient);
    }
  }

  @Test
  public void validateReplicaTableMetadataMirrorOnExistingMetadataUpdateTableFails() throws TException, IOException {
    try {
      existingReplicaTable.putToParameters(REPLICATION_EVENT.parameterName(), "previousEventId");
      existingReplicaTable.putToParameters(REPLICATION_MODE.parameterName(), ReplicationMode.METADATA_UPDATE.name());
      tableReplication.setReplicationMode(ReplicationMode.METADATA_MIRROR);
      replica = newReplica(tableReplication);
      replica.validateReplicaTable(DB_NAME, TABLE_NAME);
      fail("Should have thrown InvalidReplicationModeException");
    } catch (InvalidReplicationModeException e) {
      // Check that nothing was written to the metastore
      verify(mockMetaStoreClient).getTable(DB_NAME, TABLE_NAME);
      verify(mockMetaStoreClient).close();
      verifyNoMoreInteractions(mockMetaStoreClient);
    }
  }

  @Test
  public void validateReplicaTableMetadataMirrorOnExistingFullOverwriteReplicationTableFails()
    throws TException, IOException {
    try {
      existingReplicaTable.putToParameters(REPLICATION_EVENT.parameterName(), "previousEventId");
      existingReplicaTable.putToParameters(REPLICATION_MODE.parameterName(), FULL_OVERWRITE.name());
      tableReplication.setReplicationMode(METADATA_MIRROR);
      replica = newReplica(tableReplication);
      replica.validateReplicaTable(DB_NAME, TABLE_NAME);
      fail("Should have thrown InvalidReplicationModeException");
    } catch (InvalidReplicationModeException e) {
      // Check that nothing was written to the metastore
      verify(mockMetaStoreClient).getTable(DB_NAME, TABLE_NAME);
      verify(mockMetaStoreClient).close();
      verifyNoMoreInteractions(mockMetaStoreClient);
    }
  }

  @Test
  public void validateFullOverwriteReplicationOnExistingTableSucceeds() throws TException {
    existingReplicaTable.putToParameters(REPLICATION_EVENT.parameterName(), "previousEventId");
    existingReplicaTable.putToParameters(REPLICATION_MODE.parameterName(), FULL.name());
    tableReplication.setReplicationMode(FULL_OVERWRITE);

    replica
        .updateMetadata(EVENT_ID, tableAndStatistics,
            new PartitionsAndStatistics(sourceTable.getPartitionKeys(), Collections.<Partition>emptyList(),
                Collections.<String, List<ColumnStatisticsObj>>emptyMap()),
            DB_NAME, TABLE_NAME, mockReplicaLocationManager);
    verify(alterTableService).alterTable(eq(mockMetaStoreClient), eq(existingReplicaTable), any(Table.class));
    verify(mockMetaStoreClient).updateTableColumnStatistics(columnStatistics);
    verify(mockReplicaLocationManager, never()).addCleanUpLocation(anyString(), any(Path.class));
  }

  @Test
  public void validateFullOverwriteReplicationWithoutExistingTableFails() throws MetaException, TException {
    try {
    when(mockMetaStoreClient.tableExists(DB_NAME, TABLE_NAME)).thenReturn(false);

    tableReplication.setReplicationMode(FULL_OVERWRITE);
    replica
        .updateMetadata(EVENT_ID, tableAndStatistics,
            new PartitionsAndStatistics(sourceTable.getPartitionKeys(), Collections.<Partition>emptyList(),
                Collections.<String, List<ColumnStatisticsObj>>emptyMap()),
            DB_NAME, TABLE_NAME, mockReplicaLocationManager);

    } catch (MetaStoreClientException e) {
      // Check that nothing was written to the metastore
      verify(mockMetaStoreClient).getTable(DB_NAME, TABLE_NAME);
      verify(mockMetaStoreClient).close();
      verifyNoMoreInteractions(mockMetaStoreClient);
    }
  }

  @Test
  public void alteringExistingPartitionedReplicaTableSucceeds() throws TException, IOException {
    when(mockMetaStoreClient
        .getPartitionsByNames(DB_NAME, TABLE_NAME, Lists.newArrayList("c=one/d=two", "c=three/d=four")))
            .thenReturn(Arrays.asList(existingPartition));
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica
        .updateMetadata(EVENT_ID, tableAndStatistics,
            new PartitionsAndStatistics(sourceTable.getPartitionKeys(), Collections.<Partition>emptyList(),
                Collections.<String, List<ColumnStatisticsObj>>emptyMap()),
            DB_NAME, TABLE_NAME, mockReplicaLocationManager);
    verify(alterTableService).alterTable(eq(mockMetaStoreClient), eq(existingReplicaTable), any(Table.class));
    verify(mockMetaStoreClient).updateTableColumnStatistics(columnStatistics);
    verify(mockReplicaLocationManager, never()).addCleanUpLocation(anyString(), any(Path.class));
  }

  @Test
  public void alteringExistingPartitionedReplicaViewSucceeds() throws TException, IOException {
    convertSourceTableToView();
    convertExistingReplicaTableToView();
    when(mockMetaStoreClient
        .getPartitionsByNames(DB_NAME, TABLE_NAME, Lists.newArrayList("c=one/d=two", "c=three/d=four")))
            .thenReturn(Arrays.asList(existingPartition));
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica
        .updateMetadata(EVENT_ID, tableAndStatistics,
            new PartitionsAndStatistics(sourceTable.getPartitionKeys(), Collections.<Partition>emptyList(),
                Collections.<String, List<ColumnStatisticsObj>>emptyMap()),
            DB_NAME, TABLE_NAME, mockReplicaLocationManager);
    verify(alterTableService).alterTable(eq(mockMetaStoreClient), eq(existingReplicaTable), any(Table.class));
    verify(mockReplicaLocationManager, never()).addCleanUpLocation(anyString(), any(Path.class));
  }

  @Test(expected = CircusTrainException.class)
  public void tryToReplaceExistingPartitionedReplicaViewWithTable() throws TException, IOException {
    convertExistingReplicaTableToView();
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica
        .updateMetadata(EVENT_ID, tableAndStatistics,
            new PartitionsAndStatistics(sourceTable.getPartitionKeys(), Collections.<Partition>emptyList(),
                Collections.<String, List<ColumnStatisticsObj>>emptyMap()),
            DB_NAME, TABLE_NAME, mockReplicaLocationManager);
  }

  @Test(expected = CircusTrainException.class)
  public void tryToReplaceExistingPartitionedReplicaTableWithView() throws TException, IOException {
    convertSourceTableToView();
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica
        .updateMetadata(EVENT_ID, tableAndStatistics,
            new PartitionsAndStatistics(sourceTable.getPartitionKeys(), Collections.<Partition>emptyList(),
                Collections.<String, List<ColumnStatisticsObj>>emptyMap()),
            DB_NAME, TABLE_NAME, mockReplicaLocationManager);
  }

  @Test(expected = DestinationNotReplicaException.class)
  public void validateReplicaTableExistingNonReplicaPartitionedTableFails() throws TException, IOException {
    try {
      replica.validateReplicaTable(DB_NAME, TABLE_NAME);
    } catch (DestinationNotReplicaException e) {
      // Check that nothing was written to the metastore
      verify(mockMetaStoreClient).getTable(DB_NAME, TABLE_NAME);
      verify(mockMetaStoreClient).close();
      verifyNoMoreInteractions(mockMetaStoreClient);
      throw e;
    }
  }

  @Test
  public void getName() throws Exception {
    assertThat(replica.getName(), is(NAME));
  }

  private void alterExistingPartitionedReplicaTableWithNewPartitionsInBatches(
          int numTestAlterPartitions, int numTestAddPartitions) throws TException, IOException {
    List<Partition> existingPartitions = new ArrayList<>();
    List<Partition> newPartitions = new ArrayList<>();
    List<ColumnStatistics> modifiedPartitionStatisticsList = new ArrayList<>();
    List<ColumnStatistics> newPartitionStatisticsList = new ArrayList<>();

    for (int i = 0; i < numTestAlterPartitions; i++) {
      existingPartitions.add(newPartition(String.format("exist_%s", i), String.format("exist_%s_sub", i)));
      modifiedPartitionStatisticsList.add(
              newPartitionStatistics(String.format("exist_%s", i), String.format("exist_%s_sub", i)));
    }

    for (int i = 0; i < numTestAddPartitions; i++) {
      newPartitions.add(newPartition(String.format("new_%s", i), String.format("new_%s_sub", i)));
      newPartitionStatisticsList.add(
              newPartitionStatistics(String.format("new_%s", i), String.format("new_%s_sub", i)));
    }

    List<List<Partition>> alterSublists = Lists.partition(existingPartitions, TEST_PARTITION_BATCH_SIZE);
    int numAlterBatches = alterSublists.size();
    int lastAlterBatchSize = numAlterBatches == 0 ? 0 : alterSublists.get(alterSublists.size()-1).size();

    List<List<Partition>> addSublists = Lists.partition(newPartitions, TEST_PARTITION_BATCH_SIZE);
    int numAddBatches = addSublists.size();
    int lastAddBatchSize = numAddBatches == 0 ? 0 : addSublists.get(addSublists.size()-1).size();

    List<List<ColumnStatistics>> statsSublists = Lists.partition(
            ListUtils.union(modifiedPartitionStatisticsList, newPartitionStatisticsList), TEST_PARTITION_BATCH_SIZE);
    int numStatisticsBatches = statsSublists.size();
    int lastStatisticsBatchSize = numStatisticsBatches == 0 ? 0 : statsSublists.get(statsSublists.size()-1).size();


    List<String> testPartitionNames = new ArrayList<>();
    for (Partition p : (List<Partition>) ListUtils.union(existingPartitions, newPartitions)) {
      testPartitionNames.add(partitionName((String[]) p.getValues().toArray()));
    }

    when(mockMetaStoreClient.getPartitionsByNames(DB_NAME, TABLE_NAME, testPartitionNames))
            .thenReturn(existingPartitions);

    Map<String, List<ColumnStatisticsObj>> partitionStatsMap = new HashMap<>();
    for (int i = 0; i < numTestAddPartitions; i++) {
      partitionStatsMap
              .put(Warehouse.makePartName(PARTITIONS, newPartitions.get(i).getValues()),
                      newPartitionStatisticsList.get(i).getStatsObj());
    }
    for (int i = 0; i < numTestAlterPartitions; i++) {
      partitionStatsMap
              .put(Warehouse.makePartName(PARTITIONS, existingPartitions.get(i).getValues()),
                      modifiedPartitionStatisticsList.get(i).getStatsObj());
    }

    PartitionsAndStatistics partitionsAndStatistics = new PartitionsAndStatistics(sourceTable.getPartitionKeys(),
        ListUtils.union(existingPartitions, newPartitions), partitionStatsMap);
    for (int i = 0; i < numTestAddPartitions; i++) {
      when(mockReplicaLocationManager.getPartitionLocation(newPartitions.get(i)))
              .thenReturn(new Path(tableLocation, String.format("c=new_%s/d=new_%s_sub", i, i)));
    }
    for (int i = 0; i < numTestAlterPartitions; i++) {
      when(mockReplicaLocationManager.getPartitionLocation(existingPartitions.get(i)))
              .thenReturn(new Path(tableLocation, String.format("c=exist_%s/d=exist_%s_sub", i, i)));
    }

    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");

    replica
        .updateMetadata(EVENT_ID, tableAndStatistics, partitionsAndStatistics, DB_NAME, TABLE_NAME,
            mockReplicaLocationManager);

    verify(alterTableService).alterTable(eq(mockMetaStoreClient), eq(existingReplicaTable), any(Table.class));
    verify(mockMetaStoreClient).updateTableColumnStatistics(columnStatistics);
    verify(mockReplicaLocationManager, times(numTestAlterPartitions)).addCleanUpLocation(anyString(), any(Path.class));
    verify(mockMetaStoreClient, times(numAlterBatches)).alter_partitions(eq(DB_NAME), eq(TABLE_NAME), alterPartitionCaptor.capture());
    verify(mockMetaStoreClient, times(numAddBatches)).add_partitions(addPartitionCaptor.capture());

    // Validate that the args were expected number of batches , and expected batch sizes
    List<List<Partition>> addCaptorValues = addPartitionCaptor.getAllValues();
    assertThat(addCaptorValues.size(), is(numAddBatches));

    for (int batchNdx = 0; batchNdx < numAddBatches; batchNdx++) {
      int thisBatchSize = batchNdx < (numAddBatches - 1) ? TEST_PARTITION_BATCH_SIZE : lastAddBatchSize;
      List<Partition> addBatch = addCaptorValues.get(batchNdx);
      assertThat(addBatch.size(), is(thisBatchSize));
      for (int entryInBatchNdx = 0; entryInBatchNdx < addBatch.size(); entryInBatchNdx++) {
        assertThat(addBatch.get(entryInBatchNdx).getValues(),
                is(Arrays.asList(String.format("new_%s", (batchNdx * TEST_PARTITION_BATCH_SIZE) + entryInBatchNdx),
                        String.format("new_%s_sub", (batchNdx * TEST_PARTITION_BATCH_SIZE) + entryInBatchNdx))));
      }
    }

    List<List<Partition>> alterCaptorValues = alterPartitionCaptor.getAllValues();
    assertThat(alterCaptorValues.size(), is(numAlterBatches));
    for (int batchNdx = 0; batchNdx < numAlterBatches; batchNdx++) {
      int thisBatchSize = batchNdx < (numAlterBatches - 1) ? TEST_PARTITION_BATCH_SIZE : lastAlterBatchSize;
      List<Partition> alterBatch = alterCaptorValues.get(batchNdx);
      assertThat(alterBatch.size(), is(thisBatchSize));
      for (int entryInBatchNdx = 0; entryInBatchNdx < alterBatch.size(); entryInBatchNdx++) {
        assertThat(alterBatch.get(entryInBatchNdx).getValues(),
                is(Arrays.asList(String.format("exist_%s", (batchNdx * TEST_PARTITION_BATCH_SIZE) + entryInBatchNdx),
                        String.format("exist_%s_sub", (batchNdx * TEST_PARTITION_BATCH_SIZE) + entryInBatchNdx))));
      }
    }

    verify(mockMetaStoreClient, times(numStatisticsBatches)).setPartitionColumnStatistics(setStatsRequestCaptor.capture());
    List<SetPartitionsStatsRequest> statsRequestList = setStatsRequestCaptor.getAllValues();
    assertThat(statsRequestList.size(), is(numStatisticsBatches));

    List<ColumnStatistics> columnStats = new ArrayList<>();
    for (int colStatNdx = 0; colStatNdx < numStatisticsBatches; colStatNdx++) {
      int thisBatchSize = colStatNdx < (numStatisticsBatches - 1) ? TEST_PARTITION_BATCH_SIZE : lastStatisticsBatchSize;
      assertThat(statsRequestList.get(colStatNdx).getColStats().size(), is(thisBatchSize));
      columnStats.addAll(statsRequestList.get(colStatNdx).getColStats());
    }

    assertThat(columnStats.size(), is(numTestAlterPartitions + numTestAddPartitions));

    for (int colStatNdx = 0; colStatNdx < numTestAlterPartitions; colStatNdx++) {
      assertThat(columnStats.get(colStatNdx).getStatsDesc().isIsTblLevel(), is(false));
      assertThat(columnStats.get(colStatNdx).getStatsDesc().getDbName(), is(DB_NAME));
      assertThat(columnStats.get(colStatNdx).getStatsDesc().getTableName(), is(TABLE_NAME));
      assertThat(columnStats.get(colStatNdx).getStatsDesc().getPartName(),
              is(String.format("c=exist_%s/d=exist_%s_sub", colStatNdx, colStatNdx)));
      assertThat(columnStats.get(colStatNdx).getStatsObj().size(), is(2));
    }

    for (int colStatNdx = numTestAlterPartitions, addPartColStatNdx = 0;
         colStatNdx < numTestAlterPartitions + numTestAddPartitions;
         colStatNdx++, addPartColStatNdx++) {
      assertThat(columnStats.get(colStatNdx).getStatsDesc().isIsTblLevel(), is(false));
      assertThat(columnStats.get(colStatNdx).getStatsDesc().getDbName(), is(DB_NAME));
      assertThat(columnStats.get(colStatNdx).getStatsDesc().getTableName(), is(TABLE_NAME));
      assertThat(columnStats.get(colStatNdx).getStatsDesc().getPartName(),
              is(String.format("c=new_%s/d=new_%s_sub", addPartColStatNdx, addPartColStatNdx)));
      assertThat(columnStats.get(colStatNdx).getStatsObj().size(), is(2));
    }
  }


  @Test
  public void alteringExistingPartitionedReplicaTableWithNewPartitionsInBatchesSucceeds_0_0() throws TException, IOException {
    alterExistingPartitionedReplicaTableWithNewPartitionsInBatches(0,0);
  }

  @Test
  public void alteringExistingPartitionedReplicaTableWithNewPartitionsInBatchesSucceeds_0_1() throws TException, IOException {
    alterExistingPartitionedReplicaTableWithNewPartitionsInBatches(0,1);
  }

  @Test
  public void alteringExistingPartitionedReplicaTableWithNewPartitionsInBatchesSucceeds_1_0() throws TException, IOException {
    alterExistingPartitionedReplicaTableWithNewPartitionsInBatches(1,0);
  }

  @Test
  public void alteringExistingPartitionedReplicaTableWithNewPartitionsInBatchesSucceeds_1_1() throws TException, IOException {
    alterExistingPartitionedReplicaTableWithNewPartitionsInBatches(1,1);
  }

  @Test
  public void alteringExistingPartitionedReplicaTableWithNewPartitionsInBatchesSucceeds_boundaries() throws TException, IOException {
    alterExistingPartitionedReplicaTableWithNewPartitionsInBatches(TEST_PARTITION_BATCH_SIZE,TEST_PARTITION_BATCH_SIZE);
  }

  @Test
  public void alteringExistingPartitionedReplicaTableWithNewPartitionsInBatchesSucceeds_many() throws TException, IOException {
    alterExistingPartitionedReplicaTableWithNewPartitionsInBatches(17,28);
  }

  @Test
  public void alteringExistingPartitionedReplicaTableWithNewPartitionsInBatchesSucceeds_lots() throws TException, IOException {
    alterExistingPartitionedReplicaTableWithNewPartitionsInBatches(172,333);
  }

  @Test
  public void alteringExistingPartitionedReplicaViewWithPartitionsSucceeds() throws TException, IOException {
    convertSourceTableToView();
    convertExistingReplicaTableToView();
    Partition newPartition = newPartition("three", "four");
    newPartition.getSd().setLocation(null);
    ColumnStatistics newPartitionStatistics = newPartitionStatistics("three", "four");
    Partition modifiedPartition = new Partition(existingPartition);
    ColumnStatistics modifiedPartitionStatistics = newPartitionStatistics("one", "two");
    when(mockMetaStoreClient
        .getPartitionsByNames(DB_NAME, TABLE_NAME, Lists.newArrayList("c=one/d=two", "c=three/d=four")))
            .thenReturn(Arrays.asList(existingPartition));

    Map<String, List<ColumnStatisticsObj>> partitionStatsMap = new HashMap<>();
    partitionStatsMap
        .put(Warehouse.makePartName(PARTITIONS, newPartition.getValues()), newPartitionStatistics.getStatsObj());
    partitionStatsMap
        .put(Warehouse.makePartName(PARTITIONS, modifiedPartition.getValues()),
            modifiedPartitionStatistics.getStatsObj());

    PartitionsAndStatistics partitionsAndStatistics = new PartitionsAndStatistics(sourceTable.getPartitionKeys(),
        Arrays.asList(modifiedPartition, newPartition), partitionStatsMap);

    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");

    replica
        .updateMetadata(EVENT_ID, tableAndStatistics, partitionsAndStatistics, DB_NAME, TABLE_NAME,
            mockReplicaLocationManager);

    verify(alterTableService).alterTable(eq(mockMetaStoreClient), eq(existingReplicaTable), any(Table.class));
    verify(mockMetaStoreClient).updateTableColumnStatistics(columnStatistics);
    verify(mockMetaStoreClient).alter_partitions(eq(DB_NAME), eq(TABLE_NAME), alterPartitionCaptor.capture());
    verify(mockMetaStoreClient).add_partitions(addPartitionCaptor.capture());
    verify(mockReplicaLocationManager, never()).addCleanUpLocation(anyString(), any(Path.class));

    assertThat(alterPartitionCaptor.getValue().size(), is(1));
    assertThat(addPartitionCaptor.getValue().size(), is(1));

    Partition altered = alterPartitionCaptor.getValue().get(0);
    assertThat(altered.getValues(), is(Arrays.asList("one", "two")));

    Partition added = addPartitionCaptor.getValue().get(0);
    assertThat(added.getValues(), is(Arrays.asList("three", "four")));

    verify(mockMetaStoreClient).setPartitionColumnStatistics(setStatsRequestCaptor.capture());
    SetPartitionsStatsRequest statsRequest = setStatsRequestCaptor.getValue();

    List<ColumnStatistics> columnStats = new ArrayList<>(statsRequest.getColStats());
    Collections.sort(columnStats, new Comparator<ColumnStatistics>() {
      @Override
      public int compare(ColumnStatistics o1, ColumnStatistics o2) {
        return o1.getStatsDesc().getPartName().compareTo(o2.getStatsDesc().getPartName());
      }
    });
    assertThat(columnStats.size(), is(2));

    assertThat(columnStats.get(0).getStatsDesc().isIsTblLevel(), is(false));
    assertThat(columnStats.get(0).getStatsDesc().getDbName(), is(DB_NAME));
    assertThat(columnStats.get(0).getStatsDesc().getTableName(), is(TABLE_NAME));
    assertThat(columnStats.get(0).getStatsDesc().getPartName(), is("c=one/d=two"));
    assertThat(columnStats.get(0).getStatsObj().size(), is(2));
    assertThat(columnStats.get(1).getStatsDesc().isIsTblLevel(), is(false));
    assertThat(columnStats.get(1).getStatsDesc().getDbName(), is(DB_NAME));
    assertThat(columnStats.get(1).getStatsDesc().getTableName(), is(TABLE_NAME));
    assertThat(columnStats.get(1).getStatsDesc().getPartName(), is("c=three/d=four"));
    assertThat(columnStats.get(1).getStatsObj().size(), is(2));
  }

  @Test
  public void updateMetadataCalledWithoutPartitionsDoesNotCleanUpLocations() throws TException, IOException {
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica.updateMetadata(EVENT_ID, tableAndStatistics, DB_NAME, TABLE_NAME, mockReplicaLocationManager);
    verify(alterTableService).alterTable(eq(mockMetaStoreClient), eq(existingReplicaTable), any(Table.class));
    verify(mockMetaStoreClient).updateTableColumnStatistics(columnStatistics);
    verify(mockReplicaLocationManager, never()).addCleanUpLocation(anyString(), any(Path.class));
  }

  private Table newTable() {
    Table table = new Table();
    table.setDbName(DB_NAME);
    table.setTableName(TABLE_NAME);
    table.setTableType(TableType.EXTERNAL_TABLE.name());

    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(tableLocation);
    table.setSd(sd);

    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(StatsSetupConst.ROW_COUNT, "1");
    table.setParameters(parameters);

    table.setPartitionKeys(PARTITIONS);
    return table;
  }

  private Partition newPartition(String... values) {
    Partition partition = new Partition();
    partition.setDbName(DB_NAME);
    partition.setTableName(TABLE_NAME);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(new Path(tableLocation, partitionName(values)).toUri().toString());
    sd.setCols(FIELDS);
    partition.setSd(sd);
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(StatsSetupConst.ROW_COUNT, "1");
    partition.setParameters(parameters);
    partition.setValues(Arrays.asList(values));
    return partition;
  }

  private String partitionName(String... values) {
    try {
      return Warehouse.makePartName(PARTITIONS, Arrays.asList(values));
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }
  }

  private ColumnStatistics newPartitionStatistics(String... values) {
    ColumnStatisticsObj columnStatisticsObj1 = new ColumnStatisticsObj(COLUMN_A, "string",
        new ColumnStatisticsData(_Fields.LONG_STATS, new LongColumnStatsData(0, 1)));
    ColumnStatisticsObj columnStatisticsObj2 = new ColumnStatisticsObj(COLUMN_B, "string",
        new ColumnStatisticsData(_Fields.LONG_STATS, new LongColumnStatsData(1, 2)));
    List<ColumnStatisticsObj> columnStatisticsObjs = Arrays.asList(columnStatisticsObj1, columnStatisticsObj2);
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(false, DB_NAME, TABLE_NAME);
    statsDesc.setPartName(partitionName(values));
    return new ColumnStatistics(statsDesc, columnStatisticsObjs);
  }

}
