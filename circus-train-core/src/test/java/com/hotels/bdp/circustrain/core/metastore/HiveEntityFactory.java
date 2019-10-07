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
package com.hotels.bdp.circustrain.core.metastore;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

public final class HiveEntityFactory {

  private HiveEntityFactory() {}

  public static StorageDescriptor newStorageDescriptor(File location, String... columns) {
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = new ArrayList<>(columns.length);
    for (String name : columns) {
      cols.add(newFieldSchema(name));
    }
    sd.setCols(cols);
    sd.setSerdeInfo(new SerDeInfo());
    sd.setLocation(location.toURI().toString());
    return sd;
  }

  public static Partition newPartition(Table table, String... partitionValues) {
    Partition partition = new Partition();
    partition.setTableName(table.getTableName());
    partition.setDbName(table.getDbName());
    partition.setValues(Arrays.asList(partitionValues));
    partition.setSd(table.getSd());
    partition.setParameters(new HashMap<String, String>());
    return partition;
  }

  public static Database newDatabase(String name, String locationUri) {
    Database database = new Database();
    database.setName(name);
    database.setLocationUri(locationUri);
    return database;
  }

  public static Table newTable(String name, String dbName, List<FieldSchema> partitionKeys, StorageDescriptor sd) {
    Table table = new Table();
    table.setTableName(name);
    table.setDbName(dbName);
    table.setSd(sd);
    table.setPartitionKeys(partitionKeys);
    table.setTableType(TableType.EXTERNAL_TABLE.name());
    table.setParameters(new HashMap<String, String>());
    return table;
  }

  public static FieldSchema newFieldSchema(String name) {
    return new FieldSchema(name, "string", "");
  }
}
