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
package com.hotels.bdp.circustrain.comparator;

import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.SOURCE_LOCATION;
import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.SOURCE_METASTORE;
import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.SOURCE_TABLE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.collect.ImmutableList;

import com.hotels.bdp.circustrain.comparator.api.BaseDiff;
import com.hotels.bdp.circustrain.comparator.api.Diff;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.PartitionAndMetadata;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.TableAndMetadata;

public final class TestUtils {

  private TestUtils() {}

  public static final String METASTORE_URIS = "thrift://localhost:1234";
  public static final String DATABASE = "db";
  public static final String TABLE = "table";
  public static final String TABLE_TYPE = "EXTERNAL";
  public static final String OWNER = "owner";
  public static final int CREATE_TIME = 1234567890;
  public static final int RETENTION = 15;

  public static final String INPUT_FORMAT = "inputformat";
  public static final String OUTPUT_FORMAT = "otuputformat";

  public static final FieldSchema COL_A = new FieldSchema("a", "int", null);
  public static final FieldSchema COL_B = new FieldSchema("b", "string", null);
  public static final List<FieldSchema> COLS = ImmutableList.of(COL_A, COL_B);

  private static final String SERDE_INFO_NAME = "serdeInfo";
  private static final String SERIALIZATION_LIB = "serializationLib";

  public static TableAndMetadata newTableAndMetadata(String database, String tableName) {
    Table table = newTable(database, tableName);
    return new TableAndMetadata(Warehouse.getQualifiedName(table), table.getSd().getLocation(), table);
  }

  public static Table newTable() {
    return newTable(DATABASE, TABLE);
  }

  public static Table newTable(String database, String tableName) {
    Table table = new Table();
    table.setDbName(database);
    table.setTableName(tableName);
    table.setTableType(TABLE_TYPE);
    table.setOwner(OWNER);
    table.setCreateTime(CREATE_TIME);
    table.setRetention(RETENTION);

    Map<String, List<PrivilegeGrantInfo>> userPrivileges = new HashMap<>();
    userPrivileges.put("read", ImmutableList.of(new PrivilegeGrantInfo()));
    PrincipalPrivilegeSet privileges = new PrincipalPrivilegeSet();
    privileges.setUserPrivileges(userPrivileges);
    table.setPrivileges(privileges);

    StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(COLS);
    storageDescriptor.setInputFormat(INPUT_FORMAT);
    storageDescriptor.setOutputFormat(OUTPUT_FORMAT);
    storageDescriptor.setSerdeInfo(new SerDeInfo(SERDE_INFO_NAME, SERIALIZATION_LIB, new HashMap<String, String>()));
    storageDescriptor.setSkewedInfo(new SkewedInfo());
    storageDescriptor.setParameters(new HashMap<String, String>());
    storageDescriptor.setLocation(DATABASE + "/" + tableName + "/");
    table.setSd(storageDescriptor);

    Map<String, String> parameters = new HashMap<>();
    parameters.put("com.company.parameter", "abc");
    table.setParameters(parameters);

    return table;
  }

  public static PartitionAndMetadata newPartitionAndMetadata(String database, String tableName, String partitionValue) {
    Partition partition = newPartition(database, tableName, partitionValue);
    return new PartitionAndMetadata(partition.getDbName() + "." + partition.getTableName(),
        partition.getSd().getLocation(), partition);
  }

  public static Partition newPartition(String partitionValue) {
    return newPartition(DATABASE, TABLE, partitionValue);
  }

  public static Partition newPartition(String database, String tableName, String partitionValue) {
    Partition partition = new Partition();
    partition.setDbName(database);
    partition.setTableName(tableName);
    partition.setCreateTime(CREATE_TIME);
    partition.setValues(ImmutableList.of(partitionValue));

    Map<String, List<PrivilegeGrantInfo>> userPrivileges = new HashMap<>();
    userPrivileges.put("read", ImmutableList.of(new PrivilegeGrantInfo()));
    PrincipalPrivilegeSet privileges = new PrincipalPrivilegeSet();
    privileges.setUserPrivileges(userPrivileges);
    partition.setPrivileges(privileges);

    StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(COLS);
    storageDescriptor.setInputFormat(INPUT_FORMAT);
    storageDescriptor.setOutputFormat(OUTPUT_FORMAT);
    storageDescriptor.setSerdeInfo(new SerDeInfo(SERDE_INFO_NAME, SERIALIZATION_LIB, new HashMap<String, String>()));
    storageDescriptor.setSkewedInfo(new SkewedInfo());
    storageDescriptor.setParameters(new HashMap<String, String>());
    storageDescriptor.setLocation(DATABASE + "/" + tableName + "/" + partitionValue + "/");
    partition.setSd(storageDescriptor);

    Map<String, String> parameters = new HashMap<>();
    parameters.put("com.company.parameter", "abc");
    partition.setParameters(parameters);

    return partition;
  }

  public static Diff<Object, Object> newDiff(String message, Object left, Object right) {
    return new BaseDiff<Object, Object>(message, left, right);
  }

  public static Diff<Object, Object> newPropertyDiff(Class<?> clazz, String property, Object left, Object right) {
    return new BaseDiff<Object, Object>("Property " + property + " of class " + clazz.getName() + " is different", left,
        right);
  }

  public static void setCircusTrainSourceParameters(Table source, Table replica) {
    replica.getParameters().put(SOURCE_TABLE.parameterName(), Warehouse.getQualifiedName(source));
    replica.getParameters().put(SOURCE_LOCATION.parameterName(), source.getSd().getLocation());
    replica.getParameters().put(SOURCE_METASTORE.parameterName(), METASTORE_URIS);
  }

  public static void setCircusTrainSourceParameters(Partition sourcePartition, Partition replicaPartition) {
    replicaPartition.getParameters().put(SOURCE_TABLE.parameterName(),
        sourcePartition.getDbName() + "." + sourcePartition.getTableName());
    replicaPartition.getParameters().put(SOURCE_LOCATION.parameterName(), sourcePartition.getSd().getLocation());
    replicaPartition.getParameters().put(SOURCE_METASTORE.parameterName(), METASTORE_URIS);
  }

}
