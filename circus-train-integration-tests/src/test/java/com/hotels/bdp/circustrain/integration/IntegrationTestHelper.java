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
package com.hotels.bdp.circustrain.integration;

import static com.hotels.bdp.circustrain.integration.utils.TestUtils.newTablePartition;
import static com.hotels.bdp.circustrain.integration.utils.TestUtils.newViewPartition;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.CircusTrainTableParameter;
import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.bdp.circustrain.integration.utils.TestUtils;

public class IntegrationTestHelper {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestHelper.class);

  public static final String DATABASE = "ct_database";
  public static final String PARTITIONED_TABLE = "ct_table_p";
  public static final String SOURCE_ENCODED_TABLE = "ct_table_encoded";
  public static final String UNPARTITIONED_TABLE = "ct_table_u";
  public static final String SOURCE_MANAGED_UNPARTITIONED_TABLE = "ct_table_u_managed";
  public static final String SOURCE_MANAGED_PARTITIONED_TABLE = "ct_table_p_managed";
  public static final String SOURCE_PARTITIONED_VIEW = "ct_view_p";
  public static final String SOURCE_UNPARTITIONED_VIEW = "ct_view_u";

  public static final String PART_00000 = "part-00000";

  private final HiveMetaStoreClient metaStoreClient;

  IntegrationTestHelper(HiveMetaStoreClient metaStoreClient) {
    this.metaStoreClient = metaStoreClient;
  }

  void createPartitionedTable(URI sourceTableUri) throws Exception {
    Table hiveTable = TestUtils
        .createPartitionedTable(metaStoreClient, DATABASE, PARTITIONED_TABLE, sourceTableUri);

    URI partitionEurope = URI.create(sourceTableUri + "/continent=Europe");
    URI partitionUk = URI.create(partitionEurope + "/country=UK");
    File dataFileUk = new File(partitionUk.getPath(), PART_00000);
    FileUtils.writeStringToFile(dataFileUk, "1\tadam\tlondon\n2\tsusan\tglasgow\n");

    URI partitionAsia = URI.create(sourceTableUri + "/continent=Asia");
    URI partitionChina = URI.create(partitionAsia + "/country=China");
    File dataFileChina = new File(partitionChina.getPath(), PART_00000);
    FileUtils.writeStringToFile(dataFileChina, "1\tchun\tbeijing\n2\tpatrick\tshanghai\n");
    LOG
        .info(">>>> Partitions added: {}",
            metaStoreClient
                .add_partitions(Arrays
                    .asList(newTablePartition(hiveTable, Arrays.asList("Europe", "UK"), partitionUk),
                        newTablePartition(hiveTable, Arrays.asList("Asia", "China"), partitionChina))));
  }

  Table createParquetPartitionedTableWithStruct(
      URI tableUri,
      Schema schema,
      String structType,
      Map<String, String> structData,
      int version) throws Exception {
    List<FieldSchema> columns = Arrays.asList(
        new FieldSchema("id", "string", ""),
        new FieldSchema("details", structType, "")
    );
    List<FieldSchema> partitionKeys = Arrays.asList(new FieldSchema("hour", "string", ""));
    Table table = TestUtils
        .createPartitionedTable(metaStoreClient, DATABASE, PARTITIONED_TABLE, tableUri, columns, partitionKeys,
            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", ParquetInputFormat.class.getName(),
            ParquetOutputFormat.class.getName());
    URI partition = createData(tableUri, schema, Integer.toString(version), version, structData);
    metaStoreClient.add_partitions(Arrays.asList(newTablePartition(table,
        Arrays.asList(Integer.toString(version)), partition)));
    return metaStoreClient.getTable(DATABASE, PARTITIONED_TABLE);
  }

  URI createData(
      URI tableUri,
      Schema schema,
      String hour,
      int id,
      Map<String, String> detailsStruct) throws IOException {
    GenericData.Record record = new GenericData.Record(schema);
    Schema detailsSchema = schema.getField("details").schema();
    GenericData.Record details = new GenericData.Record(detailsSchema);
    detailsStruct.forEach(details::put);
    record.put("id", id);
    record.put("details", details);

    URI partition = URI.create(tableUri + "/hour=" + hour);
    String path = partition.getPath();
    File parentFolder = new File(path);
    parentFolder.mkdirs();
    File partitionFile = new File(parentFolder, "parquet0000");
    Path filePath = new Path(partitionFile.toURI());
    ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(filePath)
        .withSchema(schema)
        .withConf(new Configuration())
        .build();

    try {
      writer.write(record);
    } finally {
      writer.close();
    }
    return partition;
  }

  void createUnpartitionedTable(URI sourceTableUri) throws Exception {
    File dataFile = new File(sourceTableUri.getPath(), PART_00000);
    FileUtils.writeStringToFile(dataFile, "1\tadam\tlondon\n2\tsusan\tmilan\n");

    TestUtils.createUnpartitionedTable(metaStoreClient, DATABASE, UNPARTITIONED_TABLE, sourceTableUri);
  }

  void createManagedUnpartitionedTable(URI sourceTableUri) throws Exception {
    File dataFile = new File(sourceTableUri.getPath(), PART_00000);
    FileUtils.writeStringToFile(dataFile, "1\tadam\tlondon\n2\tsusan\tmilan\n");

    TestUtils.createUnpartitionedTable(metaStoreClient, DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE, sourceTableUri);
    Table table = metaStoreClient.getTable(DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE);
    table.setTableType(TableType.MANAGED_TABLE.name());
    table.putToParameters("EXTERNAL", "FALSE");
    metaStoreClient.alter_table(table.getDbName(), table.getTableName(), table);
  }

  void createManagedPartitionedTable(URI sourceTableUri) throws Exception {
    TestUtils.createPartitionedTable(metaStoreClient, DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE, sourceTableUri);
    Table table = metaStoreClient.getTable(DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE);
    table.setTableType(TableType.MANAGED_TABLE.name());
    table.putToParameters("EXTERNAL", "FALSE");
    metaStoreClient.alter_table(table.getDbName(), table.getTableName(), table);

    URI partitionEurope = URI.create(sourceTableUri + "/continent=Europe");
    URI partitionUk = URI.create(partitionEurope + "/country=UK");
    File dataFileUk = new File(partitionUk.getPath(), PART_00000);
    FileUtils.writeStringToFile(dataFileUk, "1\tadam\tlondon\n2\tsusan\tglasgow\n");

    URI partitionAsia = URI.create(sourceTableUri + "/continent=Asia");
    URI partitionChina = URI.create(partitionAsia + "/country=China");
    File dataFileChina = new File(partitionChina.getPath(), PART_00000);
    FileUtils.writeStringToFile(dataFileChina, "1\tchun\tbeijing\n2\tshanghai\tmilan\n");

    LOG
        .info(">>>> Partitions added: {}",
            metaStoreClient
                .add_partitions(Arrays
                    .asList(newTablePartition(table, Arrays.asList("Europe", "UK"), partitionUk),
                        newTablePartition(table, Arrays.asList("Asia", "China"), partitionChina))));
  }

  void createPartitionedView() throws Exception {
    Table view = TestUtils
        .createPartitionedView(metaStoreClient, DATABASE, SOURCE_PARTITIONED_VIEW, PARTITIONED_TABLE);
    metaStoreClient
        .add_partitions(Arrays
            .asList(newViewPartition(view, Arrays.asList("Europe", "UK")),
                newViewPartition(view, Arrays.asList("Asia", "China"))));
  }

  void createUnpartitionedView() throws Exception {
    TestUtils.createUnpartitionedView(metaStoreClient, DATABASE, SOURCE_UNPARTITIONED_VIEW, UNPARTITIONED_TABLE);
  }

  void createTableWithEncodedPartition(URI sourceTableUri) throws Exception {
    Table hiveTable = TestUtils.createPartitionedTable(metaStoreClient, DATABASE, SOURCE_ENCODED_TABLE, sourceTableUri);

    URI partitionEurope = URI.create(sourceTableUri + "/continent=Europe");
    URI partitionUk = URI.create(partitionEurope + "/country=U%25K");
    File dataFileUk = new File(partitionUk.getPath(), PART_00000);
    FileUtils.writeStringToFile(dataFileUk, "1\tadam\tlondon\n2\tsusan\tglasgow\n");

    LOG
        .info(">>>> Partitions added: {}",
            metaStoreClient
                .add_partitions(Arrays
                    .asList(newTablePartition(hiveTable, Arrays.asList("Europe", "U%K"),
                        URI.create(partitionEurope + "/country=U%25K")))));
  }

  public Table alterTableSetCircusTrainParameter(String database, String tableName) throws Exception {
    Table table = metaStoreClient.getTable(database, tableName);
    table.putToParameters(CircusTrainTableParameter.SOURCE_TABLE.parameterName(), database + "." + tableName);
    table.putToParameters(CircusTrainTableParameter.REPLICATION_EVENT.parameterName(), "unitTest1");
    table.putToParameters(CircusTrainTableParameter.REPLICATION_MODE.parameterName(), ReplicationMode.FULL.name());
    metaStoreClient.alter_table(database, tableName, table);
    return table;
  }
}
