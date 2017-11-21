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
package com.hotels.bdp.circustrain.core.event;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

import com.hotels.bdp.circustrain.api.event.EventPartition;
import com.hotels.bdp.circustrain.api.event.EventTable;

@RunWith(MockitoJUnitRunner.class)
public class EventUtilsTest {

  private static final List<String> PARTITION_VALUES = ImmutableList.of("P1", "P2");
  private static final List<FieldSchema> PARTITION_COLS = ImmutableList.of(new FieldSchema("P1", "string", null),
      new FieldSchema("P2", "string", null));

  private @Mock Table table;
  private @Mock StorageDescriptor tableStorageDescriptor;
  private @Mock Partition partition;
  private @Mock StorageDescriptor partitionStorageDescriptor;

  @Before
  public void init() {
    when(table.getPartitionKeys()).thenReturn(PARTITION_COLS);
    when(table.getSd()).thenReturn(tableStorageDescriptor);
    when(partition.getValues()).thenReturn(PARTITION_VALUES);
    when(partition.getSd()).thenReturn(partitionStorageDescriptor);
  }

  @Test
  public void toNullEventPartitions() {
    assertThat(EventUtils.toEventPartitions(null), is(nullValue()));
  }

  @Test
  public void toEmptyEventPartitions() {
    List<EventPartition> eventPartitions = EventUtils.toEventPartitions(Collections.<Partition> emptyList());
    assertThat(eventPartitions, is(not(nullValue())));
    assertThat(eventPartitions.size(), is(0));
  }

  @Test
  public void toEventPartitions() {
    when(partitionStorageDescriptor.getLocation()).thenReturn("location");
    List<EventPartition> eventPartitions = EventUtils.toEventPartitions(ImmutableList.of(partition));
    assertThat(eventPartitions, is(not(nullValue())));
    assertThat(eventPartitions.size(), is(1));
    assertThat(eventPartitions.get(0).getValues(), is(PARTITION_VALUES));
    assertThat(eventPartitions.get(0).getLocation(), is(URI.create("location")));
  }

  @Test
  public void toEventPartitionsWithNullLocation() {
    List<EventPartition> eventPartitions = EventUtils.toEventPartitions(ImmutableList.of(partition));
    assertThat(eventPartitions, is(not(nullValue())));
    assertThat(eventPartitions.size(), is(1));
    assertThat(eventPartitions.get(0).getValues(), is(PARTITION_VALUES));
    assertThat(eventPartitions.get(0).getLocation(), is(nullValue()));
  }

  @Test
  public void toNullEventTable() {
    assertThat(EventUtils.toEventTable(null), is(nullValue()));
  }

  @Test
  public void toEventTable() {
    when(tableStorageDescriptor.getLocation()).thenReturn("location");
    EventTable eventTable = EventUtils.toEventTable(table);
    assertThat(eventTable, is(not(nullValue())));
    assertThat(eventTable.getPartitionKeys(), is(PARTITION_VALUES));
    assertThat(eventTable.getLocation(), is(URI.create("location")));
  }

  @Test
  public void toEventTableWithNullLocation() {
    EventTable eventTable = EventUtils.toEventTable(table);
    assertThat(eventTable, is(not(nullValue())));
    assertThat(eventTable.getPartitionKeys(), is(PARTITION_VALUES));
    assertThat(eventTable.getLocation(), is(nullValue()));
  }

}
