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
package com.hotels.bdp.circustrain.api.conf;

import java.util.Locale;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;

import org.hibernate.validator.constraints.NotBlank;

public class SourceTable {

  private @NotBlank String databaseName;
  private @NotBlank String tableName;
  private String tableLocation;
  private String partitionFilter;
  private @Nullable @Min(1) Short partitionLimit;
  private boolean generatePartitionFilter = false;

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getTableLocation() {
    return tableLocation;
  }

  public void setTableLocation(String tableLocation) {
    this.tableLocation = tableLocation;
  }

  public String getPartitionFilter() {
    return partitionFilter;
  }

  public void setPartitionFilter(String partitionFilter) {
    this.partitionFilter = partitionFilter;
  }

  public Short getPartitionLimit() {
    return partitionLimit;
  }

  public void setPartitionLimit(Short partitionLimit) {
    this.partitionLimit = partitionLimit;
  }

  public String getQualifiedName() {
    return databaseName.toLowerCase(Locale.ROOT) + "." + tableName.toLowerCase(Locale.ROOT);
  }

  public boolean isGeneratePartitionFilter() {
    return generatePartitionFilter;
  }

  public void setGeneratePartitionFilter(boolean generatePartitionFilter) {
    this.generatePartitionFilter = generatePartitionFilter;
  }
}
