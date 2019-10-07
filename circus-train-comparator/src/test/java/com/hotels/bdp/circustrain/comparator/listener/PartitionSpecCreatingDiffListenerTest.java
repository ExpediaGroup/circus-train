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
package com.hotels.bdp.circustrain.comparator.listener;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import com.hotels.bdp.circustrain.comparator.api.Diff;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.TableAndMetadata;

@RunWith(MockitoJUnitRunner.class)
public class PartitionSpecCreatingDiffListenerTest {

  private static final String DB = "dbName";
  private static final String TABLE = "tableName";
  private final Configuration conf = new Configuration();
  private PartitionSpecCreatingDiffListener listener;
  @Mock
  private Table sourceTable;
  @Mock
  private Table replicaTable;
  @Mock
  private FieldSchema partitionField1;
  @Mock
  private FieldSchema partitionField2;

  private TableAndMetadata source;
  private Optional<TableAndMetadata> replica;
  private final List<Diff<Object, Object>> differences = new ArrayList<>();

  @Before
  public void setUp() {
    listener = new PartitionSpecCreatingDiffListener(conf);
    source = new TableAndMetadata(DB, "/tmp", sourceTable);
    replica = Optional.of(new TableAndMetadata(DB, "/tmp", replicaTable));
    when(partitionField1.getName()).thenReturn("p1");
    when(partitionField1.getType()).thenReturn("string");
    when(partitionField2.getName()).thenReturn("p2");
    when(partitionField2.getType()).thenReturn("smallint");
    List<FieldSchema> partitionFields = Lists.newArrayList(partitionField1, partitionField2);
    when(sourceTable.getPartitionKeys()).thenReturn(partitionFields);
  }

  @Test
  public void onNewPartition() throws Exception {
    Partition partition1 = new Partition(Lists.newArrayList("val1", "val2"), DB, TABLE, 1, 1, null, null);
    Partition partition2 = new Partition(Lists.newArrayList("val11", "val22"), DB, TABLE, 1, 1, null, null);
    listener.onDiffStart(source, replica);
    listener.onNewPartition("p1", partition1);
    listener.onNewPartition("p2", partition2);
    assertThat(listener.getPartitionSpecFilter(), is("(p1='val1' AND p2=val2) OR (p1='val11' AND p2=val22)"));
  }

  @Test
  public void onChangedPartition() throws Exception {
    Partition partition1 = new Partition(Lists.newArrayList("val1", "val2"), DB, TABLE, 1, 1, null, null);
    Partition partition2 = new Partition(Lists.newArrayList("val11", "val22"), DB, TABLE, 1, 1, null, null);
    listener.onDiffStart(source, replica);
    listener.onChangedPartition("p1", partition1, differences);
    listener.onChangedPartition("p2", partition2, differences);
    assertThat(listener.getPartitionSpecFilter(), is("(p1='val1' AND p2=val2) OR (p1='val11' AND p2=val22)"));
  }

  @Test
  public void onDataChanged() throws Exception {
    Partition partition1 = new Partition(Lists.newArrayList("val1", "val2"), DB, TABLE, 1, 1, null, null);
    Partition partition2 = new Partition(Lists.newArrayList("val11", "val22"), DB, TABLE, 1, 1, null, null);
    listener.onDiffStart(source, replica);
    listener.onDataChanged("p1", partition1);
    listener.onDataChanged("p2", partition2);
    assertThat(listener.getPartitionSpecFilter(), is("(p1='val1' AND p2=val2) OR (p1='val11' AND p2=val22)"));
  }

  @Test
  public void onChangedDataNewPartitionAndChangedPartition() throws Exception {
    Partition partition1 = new Partition(Lists.newArrayList("val1", "val2"), DB, TABLE, 1, 1, null, null);
    Partition partition2 = new Partition(Lists.newArrayList("val11", "val22"), DB, TABLE, 1, 1, null, null);
    Partition partition3 = new Partition(Lists.newArrayList("val111", "val222"), DB, TABLE, 1, 1, null, null);
    listener.onDiffStart(source, replica);
    listener.onNewPartition("p1", partition1);
    listener.onChangedPartition("p2", partition2, differences);
    listener.onDataChanged("p3", partition3);
    assertThat(listener.getPartitionSpecFilter(),
        is("(p1='val1' AND p2=val2) OR (p1='val11' AND p2=val22) OR (p1='val111' AND p2=val222)"));
  }

  @Test
  public void ignoreDuplicates() throws Exception {
    Partition partition1 = new Partition(Lists.newArrayList("val1", "val2"), DB, TABLE, 1, 1, null, null);
    Partition partition2 = new Partition(Lists.newArrayList("val11", "val22"), DB, TABLE, 1, 1, null, null);
    Partition partition3 = new Partition(Lists.newArrayList("val111", "val222"), DB, TABLE, 1, 1, null, null);
    listener.onDiffStart(source, replica);

    listener.onNewPartition("p1", partition1);
    listener.onNewPartition("p1", partition1);
    listener.onChangedPartition("p2", partition2, differences);
    listener.onChangedPartition("p2", partition2, differences);
    listener.onDataChanged("p3", partition3);
    listener.onDataChanged("p3", partition3);

    assertThat(listener.getPartitionSpecFilter(),
        is("(p1='val1' AND p2=val2) OR (p1='val11' AND p2=val22) OR (p1='val111' AND p2=val222)"));
  }

  @Test
  public void ignoreDefaultPartitionValues() throws Exception {
    Partition partition1 = new Partition(Lists.newArrayList("__HIVE_DEFAULT_PARTITION__", "val2"), DB, TABLE, 1, 1,
        null, null);
    listener.onDiffStart(source, replica);

    listener.onNewPartition("p1", partition1);

    assertThat(listener.getPartitionSpecFilter(), is(""));
  }

}
