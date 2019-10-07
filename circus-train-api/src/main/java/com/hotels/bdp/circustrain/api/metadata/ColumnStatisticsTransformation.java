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
package com.hotels.bdp.circustrain.api.metadata;

import org.apache.hadoop.hive.metastore.api.ColumnStatistics;

/**
 * Implement this class to provide transformations of {@link ColumnStatistics} metadata.
 * <p>
 * If an external implementation of this interface is found on the classpath then it will be applied to all the column
 * statistics of all replicated tables (and all their partitions if the table is partitioned).
 * </p>
 * <p>
 * The {@link ColumnStatistics} object provides information about whether it refers to table-level or partition-level
 * statistics. Some extra information about the table and the partition (in case of partition-level stats) can also be
 * extracted from the {@link ColumnStatistics} object.
 * </p>
 */
public interface ColumnStatisticsTransformation {

  public static final ColumnStatisticsTransformation IDENTITY = new ColumnStatisticsTransformation() {
    @Override
    public ColumnStatistics transform(ColumnStatistics columnStatistics) {
      return columnStatistics;
    }
  };

  ColumnStatistics transform(ColumnStatistics columnStatistics);

}
