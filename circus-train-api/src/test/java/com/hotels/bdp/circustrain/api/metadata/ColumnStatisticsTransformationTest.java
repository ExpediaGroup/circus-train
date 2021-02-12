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
package com.hotels.bdp.circustrain.api.metadata;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData._Fields;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class ColumnStatisticsTransformationTest {

  private ColumnStatistics stats;

  @Before
  public void init() {
    stats = new ColumnStatistics(new ColumnStatisticsDesc(true, "database", "table"),
        ImmutableList.of(
            new ColumnStatisticsObj("a", "int",
                new ColumnStatisticsData(_Fields.LONG_STATS, new LongColumnStatsData(1L, 2L))),
            new ColumnStatisticsObj("b", "string",
                new ColumnStatisticsData(_Fields.STRING_STATS, new StringColumnStatsData(10L, 3L, 0L, 1L)))));
  }

  @Test
  public void identity() {
    ColumnStatistics statsCopy = stats.deepCopy();
    ColumnStatistics transformedStats = ColumnStatisticsTransformation.IDENTITY.transform(stats);
    assertThat(stats, is(statsCopy)); // original stats are untouched
    assertThat(transformedStats, is(statsCopy)); // returned stats are verbatim copy of stats
    assertThat(transformedStats == stats, is(true));
  }

}
