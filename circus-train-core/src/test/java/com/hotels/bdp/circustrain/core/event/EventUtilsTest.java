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
package com.hotels.bdp.circustrain.core.event;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

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
import com.hotels.bdp.circustrain.api.event.EventPartitions;
import com.hotels.bdp.circustrain.api.event.EventTable;

@RunWith(MockitoJUnitRunner.class)
public class EventUtilsTest {

  private static final List<String> PARTITION_VALUES = ImmutableList.of("2017-01-23", "2");
  private static final List<String> PARTITION_KEY_NAMES = ImmutableList.of("local_date", "local_hour");
  private static final List<FieldSchema> PARTITION_COLS = ImmutableList
      .of(new FieldSchema(PARTITION_KEY_NAMES.get(0), "string", null), new FieldSchema(PARTITION_KEY_NAMES.get(1), "int", null));

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
    EventPartitions eventPartitions = EventUtils.toEventPartitions(table, null);
    assertThat(eventPartitions.getEventPartitions().size(), is(0));
  }

  @Test
  public void toEmptyEventPartitions() {
    EventPartitions eventPartitions = EventUtils.toEventPartitions(table, Collections.<Partition> emptyList());
    List<EventPartition> partitions = eventPartitions.getEventPartitions();
    assertThat(partitions, is(not(nullValue())));
    assertThat(partitions.size(), is(0));
    assertPartitionKeyTypes(eventPartitions.getPartitionKeyTypes());
  }

  private void assertPartitionKeyTypes(LinkedHashMap<String, String> partitionKeyTypes) {
    Iterator<Entry<String, String>> iterator = partitionKeyTypes.entrySet().iterator();
    Entry<String, String> entry = iterator.next();
    assertThat(entry.getKey(), is("local_date"));
    assertThat(entry.getValue(), is("string"));
    entry = iterator.next();
    assertThat(entry.getKey(), is("local_hour"));
    assertThat(entry.getValue(), is("int"));
    assertThat(iterator.hasNext(), is(false));
  }

  @Test
  public void toEventPartitions() {
    when(partitionStorageDescriptor.getLocation()).thenReturn("location");
    EventPartitions eventPartitions = EventUtils.toEventPartitions(table, ImmutableList.of(partition));
    List<EventPartition> partitions = eventPartitions.getEventPartitions();
    assertThat(partitions, is(not(nullValue())));
    assertThat(partitions.size(), is(1));
    assertThat(partitions.get(0).getValues(), is(PARTITION_VALUES));
    assertThat(partitions.get(0).getLocation(), is(URI.create("location")));
    assertPartitionKeyTypes(eventPartitions.getPartitionKeyTypes());
  }

  @Test
  public void toEventPartitionsWithNullLocation() {
    EventPartitions eventPartitions = EventUtils.toEventPartitions(table, ImmutableList.of(partition));
    List<EventPartition> partitions = eventPartitions.getEventPartitions();
    assertThat(partitions, is(not(nullValue())));
    assertThat(partitions.size(), is(1));
    assertThat(partitions.get(0).getValues(), is(PARTITION_VALUES));
    assertThat(partitions.get(0).getLocation(), is(nullValue()));
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
    assertThat(eventTable.getPartitionKeys(), is(PARTITION_KEY_NAMES));
    assertThat(eventTable.getLocation(), is(URI.create("location")));
  }

  @Test
  public void toEventTableWithNullLocation() {
    EventTable eventTable = EventUtils.toEventTable(table);
    assertThat(eventTable, is(not(nullValue())));
    assertThat(eventTable.getPartitionKeys(), is(PARTITION_KEY_NAMES));
    assertThat(eventTable.getLocation(), is(nullValue()));
  }

}
