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
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.Replication;
import com.hotels.bdp.circustrain.api.conf.ReplicaTable;
import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.bdp.circustrain.api.conf.SourceTable;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.copier.CopierFactory;
import com.hotels.bdp.circustrain.api.copier.CopierOptions;
import com.hotels.bdp.circustrain.api.event.CopierListener;
import com.hotels.bdp.circustrain.core.replica.Replica;
import com.hotels.bdp.circustrain.core.replica.ReplicaFactory;
import com.hotels.bdp.circustrain.core.source.Source;
import com.hotels.bdp.circustrain.core.source.SourceFactory;

@RunWith(MockitoJUnitRunner.class)
public class ReplicationFactoryImplTest {

  private static final String SOURCE_TABLE_LOCATION_BASE = "sourceTableLocationBase";
  private static final int MAX_PARTITIONS = 1;
  private static final String PARTITION_PREDICATE = "partitionPredicate";
  private static final String TABLE = "table";
  private static final String DATABASE = "database";
  private static final String TARGET_TABLE_LOCATION_BASE = "targetTableLocationBase";

  private @Mock Source source;
  private @Mock TableAndStatistics tableAndStatistics;
  private @Mock Table table;
  private @Mock Replica replica;
  private @Mock CopierFactoryManager copierFactoryManager;
  private @Mock CopierFactory copierFactory;
  private @Mock SourceTable sourceTable;
  private @Mock ReplicaTable replicaTable;
  private @Mock CopierListener copierListener;
  private @Mock PartitionPredicateFactory partitionPredicateFactory;
  private @Mock DiffGeneratedPartitionPredicate partitionPredicate;
  private @Mock ReplicaFactory replicaFactory;
  private @Mock SourceFactory sourceFactory;
  private @Mock CopierOptions copierOptions;
  private TableReplication tableReplication;
  private ReplicationFactory factory;

  @Before
  public void injectMocks() throws Exception {
    tableReplication = new TableReplication();
    tableReplication.setReplicationMode(ReplicationMode.FULL);
    tableReplication.setReplicaTable(replicaTable);
    tableReplication.setSourceTable(sourceTable);

    when(sourceFactory.newInstance(tableReplication)).thenReturn(source);
    when(source.getTableAndStatistics(DATABASE, TABLE)).thenReturn(tableAndStatistics);
    when(source.getTableAndStatistics(tableReplication)).thenReturn(tableAndStatistics);
    when(tableAndStatistics.getTable()).thenReturn(table);

    when(sourceTable.getDatabaseName()).thenReturn(DATABASE);
    when(sourceTable.getTableName()).thenReturn(TABLE);
    when(sourceTable.getTableLocation()).thenReturn(SOURCE_TABLE_LOCATION_BASE);
    when(sourceTable.getPartitionLimit()).thenReturn((short) -1);

    when(replicaTable.getTableLocation()).thenReturn(TARGET_TABLE_LOCATION_BASE);
    when(replicaTable.getDatabaseName()).thenReturn(DATABASE);
    when(replicaTable.getTableName()).thenReturn(TABLE);

    when(copierFactoryManager.getCopierFactory(any(Path.class), any(Path.class), anyMap())).thenReturn(copierFactory);
    when(partitionPredicateFactory.newInstance(tableReplication)).thenReturn(partitionPredicate);
    when(partitionPredicate.getPartitionPredicate()).thenReturn(PARTITION_PREDICATE);
    when(sourceTable.getPartitionLimit()).thenReturn((short) MAX_PARTITIONS);
    when(replicaFactory.newInstance(tableReplication)).thenReturn(replica);

    factory = new ReplicationFactoryImpl(sourceFactory, replicaFactory, copierFactoryManager, copierListener,
        partitionPredicateFactory, copierOptions);
  }

  @Test
  public void unpartitionedTableReplicationPartitionKeysNull() throws Exception {
    when(table.getPartitionKeys()).thenReturn(null);
    Replication replication = factory.newInstance(tableReplication);
    assertThat(replication, is(instanceOf(UnpartitionedTableReplication.class)));
  }

  @Test
  public void unpartitionedTableReplicationPartitionKeysEmpty() throws Exception {
    when(table.getPartitionKeys()).thenReturn(Collections.<FieldSchema> emptyList());
    Replication replication = factory.newInstance(tableReplication);
    assertThat(replication, is(instanceOf(UnpartitionedTableReplication.class)));
  }

  @Test
  public void unpartitionedTableMirrorReplication() throws Exception {
    when(table.getPartitionKeys()).thenReturn(null);
    tableReplication.setReplicationMode(ReplicationMode.METADATA_MIRROR);
    Replication replication = factory.newInstance(tableReplication);
    assertThat(replication, is(instanceOf(UnpartitionedTableMetadataMirrorReplication.class)));
  }

  @Test
  public void partitionedTableReplication() throws Exception {
    when(table.getPartitionKeys()).thenReturn(Arrays.asList(new FieldSchema()));
    Replication replication = factory.newInstance(tableReplication);
    assertEquals(PartitionedTableReplication.class, replication.getClass());
  }

  @Test
  public void partitionedTableMirrorReplication() throws Exception {
    when(table.getPartitionKeys()).thenReturn(Arrays.asList(new FieldSchema()));
    tableReplication.setReplicationMode(ReplicationMode.METADATA_MIRROR);
    Replication replication = factory.newInstance(tableReplication);
    assertThat(replication, is(instanceOf(PartitionedTableMetadataMirrorReplication.class)));
  }

  @Test
  public void unpartitionedTableMetadataUpdateReplication() throws Exception {
    when(table.getPartitionKeys()).thenReturn(null);
    tableReplication.setReplicationMode(ReplicationMode.METADATA_UPDATE);
    Replication replication = factory.newInstance(tableReplication);
    assertThat(replication, is(instanceOf(UnpartitionedTableMetadataUpdateReplication.class)));
  }

  @Test
  public void partitionedTableMetadataUpdateReplication() throws Exception {
    when(table.getPartitionKeys()).thenReturn(Arrays.asList(new FieldSchema()));
    tableReplication.setReplicationMode(ReplicationMode.METADATA_UPDATE);
    Replication replication = factory.newInstance(tableReplication);
    assertThat(replication, is(instanceOf(PartitionedTableMetadataUpdateReplication.class)));
  }

  @Test
  public void partitionedTableReplicationLazyLoadPartitionPredicate() throws Exception {
    when(table.getPartitionKeys()).thenReturn(Arrays.asList(new FieldSchema()));
    when(partitionPredicate.getPartitionPredicate()).thenThrow(new RuntimeException("Error!"));
    // Should not fail in creating a new instance.
    Replication replication = factory.newInstance(tableReplication);
    assertThat(replication, is(instanceOf(PartitionedTableReplication.class)));
  }

  @Test(expected = CircusTrainException.class)
  public void sourceDatabaseDoesNotExist() {
    when(source.getDatabase(DATABASE)).thenThrow(new CircusTrainException(""));
    factory.newInstance(tableReplication);
  }

  @Test(expected = CircusTrainException.class)
  public void sourceTableDoesNotExist() {
    when(source.getTableAndStatistics(DATABASE, TABLE)).thenThrow(new CircusTrainException(""));
    factory.newInstance(tableReplication);
  }

  @Test(expected = CircusTrainException.class)
  public void replicaDatabaseDoesNotExist() {
    when(replica.getDatabase(DATABASE)).thenThrow(new CircusTrainException(""));
    factory.newInstance(tableReplication);
  }

  @Test
  public void mergeNullOptions() {
    when(copierOptions.getCopierOptions()).thenReturn(null);
    ReplicationFactoryImpl factory = new ReplicationFactoryImpl(sourceFactory, replicaFactory, copierFactoryManager,
        copierListener, partitionPredicateFactory, copierOptions);
    Map<String, Object> mergedCopierOptions = factory.mergeCopierOptions(null);
    assertThat(mergedCopierOptions, is(not(nullValue())));
    assertThat(mergedCopierOptions.isEmpty(), is(true));
  }

  @Test
  public void mergeOptions() {
    Map<String, Object> globalOptions = new HashMap<>();
    globalOptions.put("one", Integer.valueOf(1));
    globalOptions.put("two", Integer.valueOf(2));
    when(copierOptions.getCopierOptions()).thenReturn(globalOptions);
    Map<String, Object> overrideOptions = new HashMap<>();
    overrideOptions.put("two", "two");
    overrideOptions.put("three", "three");
    ReplicationFactoryImpl factory = new ReplicationFactoryImpl(sourceFactory, replicaFactory, copierFactoryManager,
        copierListener, partitionPredicateFactory, copierOptions);
    Map<String, Object> mergedCopierOptions = factory.mergeCopierOptions(overrideOptions);
    assertThat((Integer) mergedCopierOptions.get("one"), is(Integer.valueOf(1)));
    assertThat((String) mergedCopierOptions.get("two"), is("two"));
    assertThat((String) mergedCopierOptions.get("three"), is("three"));
  }

  @Test(expected = CircusTrainException.class)
  public void viewReplicationModeIsNotMetadataMirror() {
    when(table.getTableType()).thenReturn(TableType.VIRTUAL_VIEW.name());
    factory.newInstance(tableReplication);
  }

  @Test
  public void viewReplicationModeIsMetadataMirror() {
    tableReplication.setReplicationMode(ReplicationMode.METADATA_MIRROR);
    when(table.getTableType()).thenReturn(TableType.VIRTUAL_VIEW.name());
    Replication replication = factory.newInstance(tableReplication);
    assertThat(replication, is(instanceOf(UnpartitionedTableMetadataMirrorReplication.class)));
  }

}
