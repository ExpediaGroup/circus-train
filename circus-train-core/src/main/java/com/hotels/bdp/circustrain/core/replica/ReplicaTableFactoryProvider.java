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
package com.hotels.bdp.circustrain.core.replica;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.google.common.base.Function;

import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.metadata.ColumnStatisticsTransformation;
import com.hotels.bdp.circustrain.api.metadata.PartitionTransformation;
import com.hotels.bdp.circustrain.api.metadata.TableTransformation;
import com.hotels.bdp.circustrain.core.conf.TableReplication;

@Profile({ Modules.REPLICATION })
@Component
public class ReplicaTableFactoryProvider {

  private final HiveConf sourceHiveConf;
  private final Function<Path, String> checksumFunction;
  private final TableTransformation tableTransformation;
  private final PartitionTransformation partitionTransformation;
  private final ColumnStatisticsTransformation columnStatisticsTransformation;

  @Autowired
  public ReplicaTableFactoryProvider(
      @Value("#{sourceHiveConf}") HiveConf sourceHiveConf,
      @Value("#{checksumFunction}") Function<Path, String> checksumFunction,
      TableTransformation tableTransformation,
      PartitionTransformation partitionTransformation,
      ColumnStatisticsTransformation columnStatisticsTransformation) {
    this.sourceHiveConf = sourceHiveConf;
    this.checksumFunction = checksumFunction;
    this.tableTransformation = tableTransformation;
    this.partitionTransformation = partitionTransformation;
    this.columnStatisticsTransformation = columnStatisticsTransformation;
  }

  public ReplicaTableFactory newInstance(TableReplication tableReplication) {
    if (tableReplication.getSourceTable().isGeneratePartitionFilter()) {
      return new AddCheckSumReplicaTableFactory(sourceHiveConf, checksumFunction, tableTransformation,
          partitionTransformation, columnStatisticsTransformation);
    }
    return new ReplicaTableFactory(sourceHiveConf, tableTransformation, partitionTransformation,
        columnStatisticsTransformation);
  }
}
