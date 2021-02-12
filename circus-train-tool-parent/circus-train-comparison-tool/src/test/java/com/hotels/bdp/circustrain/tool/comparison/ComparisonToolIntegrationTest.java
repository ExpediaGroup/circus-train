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
package com.hotels.bdp.circustrain.tool.comparison;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

import com.hotels.bdp.circustrain.common.test.xml.HiveXml;
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;

public class ComparisonToolIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(ComparisonToolIntegrationTest.class);
  private static final short PARTITION_LIMIT = (short) 100;
  private static final String PART_00000 = "part-00000";
  private static final String DATABASE = "ct_database";
  private static final String SOURCE_TABLE = "ct_table_1";
  private static final String REPLICA_TABLE = "ct_table_2";

  public @Rule TemporaryFolder temporaryFolder = new TemporaryFolder();
  public @Rule ThriftHiveMetaStoreJUnitRule catalog = new ThriftHiveMetaStoreJUnitRule(DATABASE);
  private File sourceTempUri;
  private File sourceWarehouseUri;
  private File sourceTableUri;
  private File replicaWarehouseUri;
  private File replicaTableUri;
  private String testHiveSiteXml;
  private File ymlFile;
  private File outputFile;

  @Before
  public void init() throws Exception {
    sourceTempUri = temporaryFolder.newFolder("source-temp");
    sourceWarehouseUri = temporaryFolder.newFolder("source-warehouse");
    sourceTableUri = new File(sourceWarehouseUri, DATABASE + "/" + SOURCE_TABLE);
    createSourceTable();

    replicaWarehouseUri = temporaryFolder.newFolder("replica-warehouse");
    replicaTableUri = new File(replicaWarehouseUri, DATABASE + "/" + REPLICA_TABLE);
    createReplicaTable();

    ymlFile = temporaryFolder.newFile("diff.yml");

    HiveMetaStoreClient client = catalog.client();
    LOG.info(">>>> Source Table = {}", client.getTable(DATABASE, SOURCE_TABLE));
    LOG.info(">>>> Replica Table = {}", client.getTable(DATABASE, REPLICA_TABLE));

    // Create hive config
    testHiveSiteXml = new HiveXml(catalog.getThriftConnectionUri(), catalog.connectionURL(), catalog.driverClassName())
        .create()
        .getCanonicalPath();

    outputFile = temporaryFolder.newFile("diff.out");
  }

  @Test
  public void typical() throws Exception {
    writeYmlFile();
    Map<String, String> props = getArgs();
    ComparisonTool.main(getArgsArray(props));
    String outputToUser = FileUtils.readFileToString(outputFile);
    assertThat(outputToUser, containsString("Partition differs"));
    assertThat(outputToUser, containsString("Table differences"));
  }

  private void createSourceTable() throws Exception {
    File partitionEurope = new File(sourceTableUri, "local_date=2000-01-01");
    File partitionUk = new File(partitionEurope, "local_hour=0");
    File dataFileUk = new File(partitionUk, PART_00000);
    FileUtils.writeStringToFile(dataFileUk, "1\tadam\tlondon\n2\tsusan\tglasgow\n");

    File partitionAsia = new File(sourceTableUri, "local_date=2000-01-02");
    File partitionChina = new File(partitionAsia, "local_hour=0");
    File dataFileChina = new File(partitionChina, PART_00000);
    String data = "1\tchun\tbeijing\n2\tshanghai\tmilan\n";
    FileUtils.writeStringToFile(dataFileChina, data);

    HiveMetaStoreClient sourceClient = catalog.client();

    Table source = new Table();
    source.setDbName(DATABASE);
    source.setTableName(SOURCE_TABLE);
    source.setTableType(TableType.EXTERNAL_TABLE.name());
    Map<String, String> parameters = new HashMap<>();
    parameters.put("comment", "comment source");
    source.setParameters(parameters);

    List<FieldSchema> partitionColumns = Arrays.asList(new FieldSchema("local_date", "string", ""),
        new FieldSchema("local_hour", "string", ""));
    source.setPartitionKeys(partitionColumns);

    List<FieldSchema> dataColumns = Arrays.asList(new FieldSchema("id", "bigint", ""),
        new FieldSchema("name", "string", ""), new FieldSchema("city", "string", ""));

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(dataColumns);
    sd.setLocation(sourceTableUri.toURI().toString());
    sd.setParameters(new HashMap<String, String>());
    sd.setSerdeInfo(new SerDeInfo());

    source.setSd(sd);

    sourceClient.createTable(source);
    LOG.info(">>>> Partitions added: {}",
        +sourceClient
            .add_partitions(Arrays.asList(newPartition(SOURCE_TABLE, sd, Arrays.asList("2000-01-01", "0"), partitionUk),
                newPartition(SOURCE_TABLE, sd, Arrays.asList("2000-01-02", "0"), partitionChina))));
  }

  private void createReplicaTable() throws Exception {
    File partitionEurope = new File(replicaTableUri, "local_date=2000-01-01");
    File partitionUk = new File(partitionEurope, "local_hour=0");
    File dataFileUk = new File(partitionUk, PART_00000);
    FileUtils.writeStringToFile(dataFileUk, "1\tadam\tlondon\tuk\n2\tsusan\tglasgow\tuk\n");

    File partitionAsia = new File(replicaTableUri, "local_date=2000-01-02");
    File partitionChina = new File(partitionAsia, "local_hour=0");
    File dataFileChina = new File(partitionChina, PART_00000);
    String data = "1\tchun\tbeijing\tchina\n2\tshanghai\tmilan\titaly\n";
    FileUtils.writeStringToFile(dataFileChina, data);

    HiveMetaStoreClient replicaClient = catalog.client();

    Table replica = new Table();
    replica.setDbName(DATABASE);
    replica.setTableName(REPLICA_TABLE);
    replica.setTableType(TableType.EXTERNAL_TABLE.name());
    Map<String, String> parameters = new HashMap<>();
    parameters.put("comment", "comment replica");
    replica.setParameters(parameters);
    List<FieldSchema> partitionColumns = Arrays.asList(new FieldSchema("local_date", "string", ""),
        new FieldSchema("local_hour", "string", ""));
    replica.setPartitionKeys(partitionColumns);

    List<FieldSchema> dataColumns = Arrays.asList(new FieldSchema("id", "bigint", ""),
        new FieldSchema("name", "string", ""), new FieldSchema("city", "string", ""),
        new FieldSchema("country", "string", ""));

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(dataColumns);
    sd.setLocation(replicaTableUri.toURI().toString());
    sd.setParameters(new HashMap<String, String>());
    sd.setSerdeInfo(new SerDeInfo());

    replica.setSd(sd);

    replicaClient.createTable(replica);
    LOG.info(">>>> Partitions added: {}",
        +replicaClient.add_partitions(
            Arrays.asList(newPartition(REPLICA_TABLE, sd, Arrays.asList("2000-01-01", "0"), partitionUk),
                newPartition(REPLICA_TABLE, sd, Arrays.asList("2000-01-02", "0"), partitionChina))));
  }

  private Partition newPartition(
      String table,
      StorageDescriptor tableStorageDescriptor,
      List<String> values,
      File location) {
    Partition partition = new Partition();
    partition.setDbName(DATABASE);
    partition.setTableName(table);
    partition.setValues(values);
    partition.setSd(new StorageDescriptor(tableStorageDescriptor));
    partition.getSd().setLocation(location.toURI().toString());
    return partition;
  }

  protected Map<String, String> getArgs() throws IOException {
    Map<String, String> props = new HashMap<>();
    props.put("config", ymlFile.getAbsolutePath());
    props.put(ComparisonToolArgs.OUTPUT_FILE, outputFile.getCanonicalPath());
    props.put(ComparisonToolArgs.SOURCE_PARTITION_BATCH_SIZE, "800");
    props.put(ComparisonToolArgs.REPLICA_PARTITION_BUFFER_SIZE, "500");
    return props;
  }

  private static String[] getArgsArray(Map<String, String> props) {
    String[] args = FluentIterable.from(props.entrySet()).transform(new Function<Entry<String, String>, String>() {
      @Override
      public String apply(Entry<String, String> input) {
        return "--" + input.getKey() + "=" + input.getValue();
      }
    }).toArray(String.class);
    return args;
  }

  private void writeYmlFile() throws IOException {
    FileUtils.writeStringToFile(ymlFile, generateYml());
  }

  private String generateYml() {
    StringBuilder yml = new StringBuilder();
    yml.append("replica-catalog:\n");
    yml.append("  name: replicaCatalogDatabaseName\n");
    yml.append("  site-xml: ").append(testHiveSiteXml).append('\n');
    yml.append("  hive-metastore-uris: ").append(catalog.getThriftConnectionUri()).append("\n");
    yml.append("source-catalog:\n");
    yml.append("  name: source-").append(catalog.databaseName()).append('\n');
    yml.append("  temporary-path: ").append(sourceTempUri.toURI().toString()).append('\n');
    yml.append("  disable-snapshots: true\n");
    yml.append("  site-xml: ").append(testHiveSiteXml).append('\n');
    yml.append("  hive-metastore-uris: ").append(catalog.getThriftConnectionUri()).append("\n");
    yml.append("copier-options:\n");
    yml.append("  file-attribute: replication, blocksize, user, group, permission, checksumtype\n");
    yml.append("  preserve-raw-xattrs: false\n");
    yml.append("table-replications:\n");
    yml.append("  -\n");
    yml.append("    source-table:\n");
    yml.append("      database-name: ").append(DATABASE).append('\n');
    yml.append("      table-name: ").append(SOURCE_TABLE).append('\n');
    yml.append("      partition-filter: local_date >= '1970-01-01'\n");
    yml.append("      partition-limit: ").append(String.valueOf(PARTITION_LIMIT));
    yml.append('\n');
    yml.append("    replica-table:\n");
    yml.append("      database-name: ").append(DATABASE).append('\n');
    yml.append("      table-name: ").append(REPLICA_TABLE).append('\n');
    yml.append("      table-location: ").append(replicaTableUri.toURI().toString()).append('\n');
    return yml.toString();
  }

}
