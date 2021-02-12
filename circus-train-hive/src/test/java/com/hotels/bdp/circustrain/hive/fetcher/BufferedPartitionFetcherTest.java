/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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
package com.hotels.bdp.circustrain.hive.fetcher;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BufferedPartitionFetcherTest {

  private static final String TABLE_NAME = "table";
  private static final String DATABASE_NAME = "database";

  private @Mock IMetaStoreClient metastore;
  private @Mock Table table;

  private @Mock Partition p01;
  private @Mock Partition p02;
  private @Mock Partition p03;

  private void init() throws Exception {
    when(p01.getValues()).thenReturn(Arrays.asList("01"));
    when(p02.getValues()).thenReturn(Arrays.asList("02"));
    when(p03.getValues()).thenReturn(Arrays.asList("03"));

    when(table.getDbName()).thenReturn(DATABASE_NAME);
    when(table.getTableName()).thenReturn(TABLE_NAME);
    when(table.getPartitionKeys()).thenReturn(Arrays.asList(new FieldSchema("a", "String", null)));

    when(metastore.listPartitionNames(DATABASE_NAME, TABLE_NAME, (short) -1))
        .thenReturn(Arrays.asList("a=01", "a=02", "a=03"));
  }

  @Test(expected = PartitionNotFoundException.class)
  public void unknowPartition() throws Exception {
    BufferedPartitionFetcher fetcher = spy(new BufferedPartitionFetcher(metastore, table, (short) 10));

    fetcher.fetch("a=10");
  }

  @Test
  public void all() throws Exception {
    init();
    when(metastore.getPartitionsByNames(DATABASE_NAME, TABLE_NAME, Arrays.asList("a=01", "a=02", "a=03")))
        .thenReturn(Arrays.asList(p01, p02, p03));

    BufferedPartitionFetcher fetcher = spy(new BufferedPartitionFetcher(metastore, table, (short) 3));

    assertThat(fetcher.fetch("a=01"), is(p01));
    assertThat(fetcher.fetch("a=03"), is(p03));
    verify(fetcher, times(1)).bufferPartitions(0);
  }

  @Test
  public void longBuffer() throws Exception {
    init();
    when(metastore.getPartitionsByNames(DATABASE_NAME, TABLE_NAME, Arrays.asList("a=01", "a=02", "a=03")))
        .thenReturn(Arrays.asList(p01, p02, p03));

    BufferedPartitionFetcher fetcher = spy(new BufferedPartitionFetcher(metastore, table, (short) 5));

    assertThat(fetcher.fetch("a=01"), is(p01));
    assertThat(fetcher.fetch("a=03"), is(p03));
    verify(fetcher, times(1)).bufferPartitions(0);
  }

  @Test
  public void shortBuffer() throws Exception {
    init();
    when(metastore.getPartitionsByNames(DATABASE_NAME, TABLE_NAME, Arrays.asList("a=01", "a=02")))
        .thenReturn(Arrays.asList(p01, p02));
    when(metastore.getPartitionsByNames(DATABASE_NAME, TABLE_NAME, Arrays.asList("a=03")))
        .thenReturn(Arrays.asList(p03));

    BufferedPartitionFetcher fetcher = spy(new BufferedPartitionFetcher(metastore, table, (short) 2));

    assertThat(fetcher.fetch("a=01"), is(p01));
    verify(fetcher, times(1)).bufferPartitions(0);

    assertThat(fetcher.fetch("a=03"), is(p03));
    verify(fetcher, times(1)).bufferPartitions(2);
  }

}
