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
package com.hotels.bdp.circustrain.core.replica;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.PARTITION_CHECKSUM;
import static com.hotels.bdp.circustrain.api.conf.ReplicationMode.FULL;
import static com.hotels.bdp.circustrain.core.metastore.HiveEntityFactory.newFieldSchema;
import static com.hotels.bdp.circustrain.core.metastore.HiveEntityFactory.newPartition;
import static com.hotels.bdp.circustrain.core.metastore.HiveEntityFactory.newStorageDescriptor;
import static com.hotels.bdp.circustrain.core.metastore.HiveEntityFactory.newTable;

import java.io.File;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import com.hotels.bdp.circustrain.api.metadata.ColumnStatisticsTransformation;
import com.hotels.bdp.circustrain.api.metadata.PartitionTransformation;
import com.hotels.bdp.circustrain.api.metadata.TableTransformation;

@RunWith(MockitoJUnitRunner.class)
public class AddCheckSumReplicaTableFactoryTest {

  private @Mock HiveConf sourceHiveConf;
  private @Mock Function<Path, String> checksumFunction;

  private Table sourceTable;
  private Partition sourcePartition;
  private final Path replicaPartitionLocation = new Path("/tmp/db/table/partitionKey=value1");
  private AddCheckSumReplicaTableFactory factory;
  private final File sourcePartitionFile = new File("/tmp/db/tab%le/partitionKey=value1");

  @Before
  public void setUp() {
    sourceTable = newTable("name", "dbName", Lists.newArrayList(newFieldSchema("partitionKey")),
        newStorageDescriptor(new File("/tmp/db/tab%le"), "column1"));
    sourcePartition = newPartition(sourceTable, "value1");
    sourcePartition.setSd(newStorageDescriptor(sourcePartitionFile, "column1"));
    sourcePartition.setParameters(new HashMap<String, String>());
    factory = new AddCheckSumReplicaTableFactory(sourceHiveConf, checksumFunction, TableTransformation.IDENTITY,
        PartitionTransformation.IDENTITY, ColumnStatisticsTransformation.IDENTITY);
  }

  @Test
  public void newReplicaPartition() throws Exception {
    Path sourceTableLocationPath = new Path(sourcePartitionFile.toURI().toString());
    when(checksumFunction.apply(sourceTableLocationPath)).thenReturn("checksum");

    Partition partition = factory.newReplicaPartition("eventId", sourceTable, sourcePartition, "replicaDatabase",
        "replicaTable", replicaPartitionLocation, FULL);
    assertThat(partition.getParameters().get(PARTITION_CHECKSUM.parameterName()), is("checksum"));
  }
}
