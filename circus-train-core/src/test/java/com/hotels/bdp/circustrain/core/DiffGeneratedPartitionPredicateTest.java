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

import static com.hotels.bdp.circustrain.core.HiveEntityFactory.newFieldSchema;
import static com.hotels.bdp.circustrain.core.HiveEntityFactory.newPartition;
import static com.hotels.bdp.circustrain.core.HiveEntityFactory.newStorageDescriptor;
import static com.hotels.bdp.circustrain.core.HiveEntityFactory.newTable;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.core.conf.SourceTable;
import com.hotels.bdp.circustrain.core.conf.TableReplication;
import com.hotels.bdp.circustrain.core.replica.Replica;
import com.hotels.bdp.circustrain.core.source.Source;
import com.hotels.hcommon.hive.metastore.client.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.DefaultMetaStoreClientSupplier;
import com.hotels.hcommon.hive.metastore.client.MetaStoreClientFactory;

@RunWith(MockitoJUnitRunner.class)
public class DiffGeneratedPartitionPredicateTest {

  private @Mock Source source;
  private @Mock Replica replica;
  private @Mock TableReplication tableReplication;
  private @Mock SourceTable sourceTable;
  private @Mock TableAndStatistics sourceTableAndStats;
  private @Mock TableAndStatistics replicaTableAndStats;
  private @Mock Function<Path, String> checksumFunction;
  private @Mock MetaStoreClientFactory factory;
  private @Mock CloseableMetaStoreClient client;

  private Supplier<CloseableMetaStoreClient> supplier;
  private Table table1;
  private List<Partition> table1Partitions;
  private List<String> table1PartitionNames;
  private Table table2;
  private DiffGeneratedPartitionPredicate predicate;

  @Before
  public void setUp() throws Exception {
    setupHiveTables();
    when(client.listPartitionNames(table1.getDbName(), table1.getTableName(), (short) -1))
        .thenReturn(table1PartitionNames);
    when(client.getPartitionsByNames(table1.getDbName(), table1.getTableName(), table1PartitionNames))
        .thenReturn(table1Partitions);
    when(factory.newInstance(any(HiveConf.class), anyString())).thenReturn(client);
    when(tableReplication.getSourceTable()).thenReturn(sourceTable);
    when(tableReplication.getPartitionIteratorBatchSize()).thenReturn((short) 100);
    when(tableReplication.getPartitionFetcherBufferSize()).thenReturn((short) 100);
    // HiveConf hiveConf = new HiveConf(catalog.conf(), HiveMetaStoreClient.class);
    HiveConf hiveConf = new HiveConf();
    supplier = new DefaultMetaStoreClientSupplier(hiveConf, "test", factory);
    when(source.getHiveConf()).thenReturn(hiveConf);
    predicate = new DiffGeneratedPartitionPredicate(source, replica, tableReplication, checksumFunction);
    when(source.getMetaStoreClientSupplier()).thenReturn(supplier);
    when(replica.getMetaStoreClientSupplier()).thenReturn(supplier);
    when(source.getMetaStoreClientSupplier()).thenReturn(supplier);
    when(replica.getMetaStoreClientSupplier()).thenReturn(supplier);
    when(source.getTableAndStatistics(tableReplication)).thenReturn(sourceTableAndStats);
    when(sourceTableAndStats.getTable()).thenReturn(table1);
  }

  @Test
  public void autogeneratePredicate() throws Exception {
    when(replica.getTableAndStatistics(tableReplication)).thenReturn(replicaTableAndStats);
    when(replicaTableAndStats.getTable()).thenReturn(table2);

    assertThat(predicate.getPartitionPredicate(),
        is("(p1='value1' AND p2='value2') OR (p1='value11' AND p2='value22')"));
  }

  @Test
  public void autogeneratePredicateReplicaTableDoesNotExist() throws Exception {
    when(replica.getTableAndStatistics(tableReplication)).thenThrow(new CircusTrainException("Table does not exist!"));

    assertThat(predicate.getPartitionPredicate(),
        is("(p1='value1' AND p2='value2') OR (p1='value11' AND p2='value22')"));
  }

  @Test
  public void partitionPredicateLimit() throws Exception {
    when(replica.getTableAndStatistics(tableReplication)).thenReturn(replicaTableAndStats);
    when(replicaTableAndStats.getTable()).thenReturn(table2);
    when(sourceTable.getPartitionLimit()).thenReturn((short) 10);

    assertThat(predicate.getPartitionPredicateLimit(), is((short) 10));
  }

  @Test
  public void noPartitionPredicateLimitSetDefaultsToMinus1() throws Exception {
    when(replica.getTableAndStatistics(tableReplication)).thenReturn(replicaTableAndStats);
    when(replicaTableAndStats.getTable()).thenReturn(table2);
    when(sourceTable.getPartitionLimit()).thenReturn(null);

    assertThat(predicate.getPartitionPredicateLimit(), is((short) -1));
  }

  @Test
  public void partitionPredicateLimitOverriddenToZeroWhenNoDiffs() throws Exception {
    when(sourceTableAndStats.getTable()).thenReturn(table2);// no partitions so no diff
    when(replica.getTableAndStatistics(tableReplication)).thenReturn(replicaTableAndStats);
    when(replicaTableAndStats.getTable()).thenReturn(table2);
    when(sourceTable.getPartitionLimit()).thenReturn((short) 10);

    assertThat(predicate.getPartitionPredicateLimit(), is((short) 0));
  }

  private void setupHiveTables() throws TException, IOException {
    List<FieldSchema> partitionKeys = Lists.newArrayList(newFieldSchema("p1"), newFieldSchema("p2"));

    File tableLocation = new File("db1", "table1");
    StorageDescriptor sd = newStorageDescriptor(tableLocation, "col0");
    table1 = newTable("table1", "db1", partitionKeys, sd);
    Partition partition1 = newPartition(table1, "value1", "value2");
    Partition partition2 = newPartition(table1, "value11", "value22");
    table1Partitions = Arrays.asList(partition1, partition2); //
    table1PartitionNames = Arrays.asList(Warehouse.makePartName(partitionKeys, partition1.getValues()),
        Warehouse.makePartName(partitionKeys, partition2.getValues()));

    File tableLocation2 = new File("db2", "table2");
    StorageDescriptor sd2 = newStorageDescriptor(tableLocation2, "col0");
    table2 = newTable("table2", "db2", partitionKeys, sd2);
  }
}
