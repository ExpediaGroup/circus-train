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
package com.hotels.bdp.circustrain.comparator.hive.wrappers;

import org.apache.hadoop.hive.metastore.api.Partition;

import com.google.common.base.Objects;

public class PartitionAndMetadata {

  private final String sourceTable;
  private final String sourceLocation;
  private final Partition partition;

  public PartitionAndMetadata(String sourceTable, String sourceLocation, Partition partition) {
    this.sourceTable = sourceTable;
    this.sourceLocation = sourceLocation;
    this.partition = partition;
  }

  public String getSourceTable() {
    return sourceTable;
  }

  public String getSourceLocation() {
    return sourceLocation;
  }

  public Partition getPartition() {
    return partition;
  }

  @Override
  public String toString() {
    return Objects
        .toStringHelper(this)
        .add("sourceTable", sourceTable)
        .add("sourceLocation", sourceLocation)
        .add("partition", partition)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(sourceTable, sourceLocation, partition);
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
    PartitionAndMetadata other = (PartitionAndMetadata) obj;
    return Objects.equal(sourceTable, other.sourceTable)
        && Objects.equal(sourceLocation, other.sourceLocation)
        && Objects.equal(partition, other.partition);
  }

}
