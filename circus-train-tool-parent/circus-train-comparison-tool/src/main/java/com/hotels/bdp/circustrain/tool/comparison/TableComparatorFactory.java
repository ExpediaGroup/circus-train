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
package com.hotels.bdp.circustrain.tool.comparison;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.comparator.ComparatorRegistry;
import com.hotels.bdp.circustrain.comparator.api.DiffListener;
import com.hotels.bdp.circustrain.core.HiveEndpoint;

@Component
class TableComparatorFactory {

  private final HiveEndpoint source;
  private final HiveEndpoint replica;
  private final ComparatorRegistry comparatorRegistry;
  private final DiffListener diffListener;
  private final short sourcePartitionBatchSize;
  private final short replicaPartitionBufferSize;

  @Autowired
  TableComparatorFactory(
      HiveEndpoint source,
      HiveEndpoint replica,
      ComparatorRegistry comparatorRegistry,
      DiffListener diffListener,
      @Value("${sourcePartitionBatchSize:1000}") short sourcePartitionBatchSize,
      @Value("${replicaPartitionBufferSize:1000}") short replicaPartitionBufferSize) {
    this.sourcePartitionBatchSize = sourcePartitionBatchSize;
    this.replicaPartitionBufferSize = replicaPartitionBufferSize;
    this.source = source;
    this.replica = replica;
    this.comparatorRegistry = comparatorRegistry;
    this.diffListener = diffListener;
  }

  public TableComparator newInstance(TableReplication tableReplication) {
    return new TableComparator(source, replica, comparatorRegistry, diffListener, tableReplication,
        sourcePartitionBatchSize, replicaPartitionBufferSize);
  }

}
