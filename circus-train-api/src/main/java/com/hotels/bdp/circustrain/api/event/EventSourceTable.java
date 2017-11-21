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
package com.hotels.bdp.circustrain.api.event;

public class EventSourceTable {

  private final String databaseName;
  private final String tableName;
  private final String tableLocation;
  private final String partitionFilter;
  private final Short partitionLimit;
  private final String qualifiedName;

  public EventSourceTable(
      String databaseName,
      String tableName,
      String tableLocation,
      String partitionFilter,
      Short partitionLimit,
      String qualifiedName) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.tableLocation = tableLocation;
    this.partitionFilter = partitionFilter;
    this.partitionLimit = partitionLimit;
    this.qualifiedName = qualifiedName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTableLocation() {
    return tableLocation;
  }

  public String getPartitionFilter() {
    return partitionFilter;
  }

  public Short getPartitionLimit() {
    return partitionLimit;
  }

  public String getQualifiedName() {
    return qualifiedName;
  }
}
