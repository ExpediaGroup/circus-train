/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
package com.hotels.bdp.circustrain.core;

import com.hotels.bdp.circustrain.api.conf.ReplicaTable;
import com.hotels.bdp.circustrain.api.conf.SourceTable;
import com.hotels.bdp.circustrain.api.conf.TableReplication;

public final class TableReplicationUtils {

  private TableReplicationUtils() {};

  protected static TableReplication createTableReplication(
      String sourceDatabaseName,
      String sourceTableName,
      String replicaDatabaseName,
      String replicateTableName,
      String targetTableLocation) {
    TableReplication tableReplication = new TableReplication();
    ReplicaTable replicaTable = new ReplicaTable();
    replicaTable.setDatabaseName(replicaDatabaseName);
    replicaTable.setTableName(replicateTableName);
    replicaTable.setTableLocation(targetTableLocation);
    tableReplication.setReplicaTable(replicaTable);
    SourceTable sourceTable = new SourceTable();
    sourceTable.setDatabaseName(sourceDatabaseName);
    sourceTable.setTableName(sourceTableName);
    tableReplication.setSourceTable(sourceTable);
    return tableReplication;
  }

}
