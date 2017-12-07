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
package com.hotels.bdp.circustrain.comparator.api;

public enum ExcludedHiveParameter {
  TRANSIENT_LAST_DDL_TIME("transient_lastDdlTime"),
  COMMENT("comment"),
  DO_NOT_UPDATE_STATS("DO_NOT_UPDATE_STATS"),
  STATS_GENERATED_VIA_STATS_TASK("STATS_GENERATED_VIA_STATS_TASK"),
  STATS_GENERATEDK(org.apache.hadoop.hive.common.StatsSetupConst.STATS_GENERATED),
  COLUMN_STATS_ACCURATE(org.apache.hadoop.hive.common.StatsSetupConst.COLUMN_STATS_ACCURATE),
  // Seen cases where this is in the replica but not in the source most likely they are added by hive when doing the
  // replication, so ignoring them.
  NUM_FILES(org.apache.hadoop.hive.common.StatsSetupConst.NUM_FILES),
  // Seen cases where this is in the replica but not in the source most likely they are added by hive when doing the
  // replication, so ignoring them.
  TOTAL_SIZE(org.apache.hadoop.hive.common.StatsSetupConst.TOTAL_SIZE),
  // Table type
  EXTERNAL("EXTERNAL");

  private final String parameterName;

  private ExcludedHiveParameter(String parameterName) {
    this.parameterName = parameterName;

  }

  public String parameterName() {
    return parameterName;
  }

}
