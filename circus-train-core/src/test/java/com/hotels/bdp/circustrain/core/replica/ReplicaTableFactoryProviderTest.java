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
package com.hotels.bdp.circustrain.core.replica;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Function;

import com.hotels.bdp.circustrain.api.metadata.ColumnStatisticsTransformation;
import com.hotels.bdp.circustrain.api.metadata.PartitionTransformation;
import com.hotels.bdp.circustrain.api.metadata.TableTransformation;
import com.hotels.bdp.circustrain.conf.SourceTable;
import com.hotels.bdp.circustrain.conf.TableReplication;

@RunWith(MockitoJUnitRunner.class)
public class ReplicaTableFactoryProviderTest {

  private @Mock HiveConf sourceHiveConf;
  private @Mock Function<Path, String> checksumFunction;
  private @Mock TableReplication tableReplication;
  private @Mock SourceTable sourceTable;
  private @Mock TableTransformation tableTransformation;
  private @Mock PartitionTransformation partitionTransformation;
  private @Mock ColumnStatisticsTransformation columnStatisticsTransformation;

  private ReplicaTableFactoryProvider picker;

  @Before
  public void setUp() {
    picker = new ReplicaTableFactoryProvider(sourceHiveConf, checksumFunction, tableTransformation,
        partitionTransformation, columnStatisticsTransformation);
    when(tableReplication.getSourceTable()).thenReturn(sourceTable);
  }

  @Test
  public void newInstanceReturnsReplicaTableFactory() throws Exception {
    when(sourceTable.isGeneratePartitionFilter()).thenReturn(false);
    ReplicaTableFactory factory = picker.newInstance(tableReplication);
    assertThat(factory, instanceOf(ReplicaTableFactory.class));
    assertThat(factory, not(instanceOf(AddCheckSumReplicaTableFactory.class)));
  }

  @Test
  public void newInstanceReturnsAddChecksumReplicaTableFactory() throws Exception {
    when(sourceTable.isGeneratePartitionFilter()).thenReturn(true);
    ReplicaTableFactory factory = picker.newInstance(tableReplication);
    assertThat(factory, instanceOf(AddCheckSumReplicaTableFactory.class));
  }
}
