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
package com.hotels.bdp.circustrain.comparator.hive.wrappers;

import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.base.Objects;

public class TableAndMetadata {

  private final String sourceTable;
  private final String sourceLocation;
  private final Table table;

  public TableAndMetadata(String sourceTable, String sourceLocation, Table table) {
    this.sourceTable = sourceTable;
    this.sourceLocation = sourceLocation;
    this.table = table;
  }

  public String getSourceTable() {
    return sourceTable;
  }

  public String getSourceLocation() {
    return sourceLocation;
  }

  public Table getTable() {
    return table;
  }

  @Override
  public String toString() {
    return Objects
        .toStringHelper(this)
        .add("sourceTable", sourceTable)
        .add("sourceLocation", sourceLocation)
        .add("table", table)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(sourceTable, sourceLocation, table);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TableAndMetadata other = (TableAndMetadata) obj;
    return Objects.equal(sourceTable, other.sourceTable)
        && Objects.equal(sourceLocation, other.sourceLocation)
        && Objects.equal(table, other.table);
  }

}
