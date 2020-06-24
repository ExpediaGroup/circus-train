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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import static com.hotels.bdp.circustrain.integration.IntegrationTestHelper.EVOLUTION_COLUMN;
import static com.hotels.bdp.circustrain.integration.IntegrationTestHelper.PARTITIONED_TABLE;
import static com.hotels.bdp.circustrain.integration.utils.TestUtils.newTablePartition;
import static com.hotels.bdp.circustrain.integration.utils.TestUtils.toUri;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.avro.AvroObjectInspectorGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fm.last.commons.test.file.ClassDataFolder;
import fm.last.commons.test.file.DataFolder;

import com.google.common.collect.Lists;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;

import com.hotels.bdp.circustrain.common.test.base.CircusTrainRunner;
import com.hotels.bdp.circustrain.common.test.junit.rules.ServerSocketRule;
import com.hotels.bdp.circustrain.integration.utils.SchemaEvolution;
import com.hotels.bdp.circustrain.integration.utils.ThriftMetastoreServerRuleExtension;
import com.hotels.hcommon.hive.metastore.iterator.PartitionIterator;

@Category(SchemaEvolution.class)
@RunWith(StandaloneHiveRunner.class)
public class CircusTrainParquetSchemaEvolutionIntegrationTest {

  private static String SOURCE_DB = "source_db";
  private static String TABLE = "ct_table_p";
  private static String REPLICA_DB = "replica_db";

  private static final Logger log = LoggerFactory.getLogger(CircusTrainParquetSchemaEvolutionIntegrationTest.class);

  @HiveSQL(files = {}, autoStart = false)
  private HiveShell shell;

  public @Rule ExpectedSystemExit exit = ExpectedSystemExit.none();
  public @Rule TemporaryFolder temporaryFolder = new TemporaryFolder();
  public @Rule DataFolder dataFolder = new ClassDataFolder();
  public @Rule ServerSocketRule serverSocketRule = new ServerSocketRule();

  private File sourceWarehouseUri;
  private File replicaWarehouseUri;
  private File housekeepingDbLocation;

  private IntegrationTestHelper helper;

  private ThriftMetastoreServerRuleExtension thriftMetaStoreRule;
  private HiveMetaStoreClient metaStoreClient;

  @Before
  public void init() throws Throwable {
    shell.start();
    shell.execute("CREATE DATABASE " + SOURCE_DB);
    shell.execute("CREATE DATABASE " + REPLICA_DB);
    HiveConf hiveConf = shell.getHiveConf();
    thriftMetaStoreRule = new ThriftMetastoreServerRuleExtension(hiveConf);
    thriftMetaStoreRule.before();
    metaStoreClient = thriftMetaStoreRule.client();

    sourceWarehouseUri = temporaryFolder.newFolder("source-warehouse");
    replicaWarehouseUri = temporaryFolder.newFolder("replica-warehouse");
    helper = new IntegrationTestHelper(metaStoreClient);
    temporaryFolder.newFolder("db");
    housekeepingDbLocation = new File(new File(temporaryFolder.getRoot(), "db"), "housekeeping");
  }

  @After
  public void teardown() {
    thriftMetaStoreRule.after();
  }

  @Test
  public void addFieldAtEnd() throws Exception {
    Schema schema = getSchemaFieldAssembler().endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredString(EVOLUTION_COLUMN)
        .endRecord();
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "after");
    List<String> expectedData = Lists.newArrayList(
        "1\tNULL\t1",
        "2\tafter\t2"
    );
    runTest(schema, evolvedSchema, new FieldDataWrapper(), afterEvolution);
  }

  @Test
  public void removeField() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredString(EVOLUTION_COLUMN)
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "before");
    List<String> expectedData = Lists.newArrayList(
        "1\t1",
        "2\t2"
    );
    runTest(schema, evolvedSchema, beforeEvolution, new FieldDataWrapper());
  }

  // Replication success - old expectedData not "renamed" too
  @Test
  public void renameField() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredString(EVOLUTION_COLUMN)
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredString(EVOLUTION_COLUMN + "_renamed")
        .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "before");
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN + "_renamed", "after");
    List<String> expectedData = Lists.newArrayList(
        "1\tbefore\t1",
        "2\tafter\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test
  public void addDefaultValue() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredString(EVOLUTION_COLUMN)
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().nullableString(EVOLUTION_COLUMN, "default")
        .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "before");
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "after");
    List<String> expectedData = Lists.newArrayList(
        "1\tbefore\t1",
        "2\tafter\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test
  public void removeDefaultValue() throws Exception {
    Schema schema = getSchemaFieldAssembler().nullableString(EVOLUTION_COLUMN, "default")
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredString(EVOLUTION_COLUMN)
        .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "before");
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "after");
    List<String> expectedData = Lists.newArrayList(
        "1\tbefore\t1",
        "2\tafter\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test
  public void makeFieldNullable() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredString(EVOLUTION_COLUMN)
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().name(EVOLUTION_COLUMN)
        .type()
        .nullable()
        .stringType()
        .noDefault()
        .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "before");
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, null);
    List<String> expectedData = Lists.newArrayList(
        "1\tbefore\t1",
        "2\tNULL\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test
  public void makeFieldStruct() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredString(EVOLUTION_COLUMN)
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().name(EVOLUTION_COLUMN)
        .type()
        .record(EVOLUTION_COLUMN + "_struct")
        .fields()
        .requiredString("after_col")
        .endRecord()
        .noDefault()
        .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "before");
    Map<String, String> after = new HashMap<>();
    after.put("after_col", "after");
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, after);
    List<String> expectedData = Lists.newArrayList(
        "1\tbefore\t1",
        "2\ttrue\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test
  public void addColumnToStruct() throws Exception {
    Schema schema = getSchemaFieldAssembler().name(EVOLUTION_COLUMN)
        .type()
        .record(EVOLUTION_COLUMN + "_struct")
        .fields()
        .requiredString("name")
        .requiredString("city")
        .endRecord()
        .noDefault()
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().name(EVOLUTION_COLUMN)
        .type()
        .record(EVOLUTION_COLUMN + "_struct")
        .fields()
        .requiredString("name")
        .requiredString("city")
        .requiredString("dob")
        .endRecord()
        .noDefault()
        .endRecord();
    Map<String, String> before = new HashMap<>();
    before.put("name", "lisa");
    before.put("city", "blackpool");
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, before);
    Map<String, String> after = new HashMap<>();
    after.put("name", "adam");
    after.put("city", "london");
    after.put("dob", "22/09/1992");
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, after);
    List<String> expectedData = Lists.newArrayList(
        "1\t{\"name\":\"lisa\",\"city\":\"blackpool\",\"dob\":null}\t1",
        "2\t{\"name\":\"adam\",\"city\":\"london\",\"dob\":\"22/09/1992\"}\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test
  public void removeColumnFromStruct() throws Exception {
    Schema schema = getSchemaFieldAssembler().name(EVOLUTION_COLUMN)
        .type()
        .record(EVOLUTION_COLUMN + "_struct")
        .fields()
        .requiredString("name")
        .requiredString("city")
        .requiredString("dob")
        .endRecord()
        .noDefault()
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().name(EVOLUTION_COLUMN)
        .type()
        .record(EVOLUTION_COLUMN + "_struct")
        .fields()
        .requiredString("name")
        .requiredString("city")
        .endRecord()
        .noDefault()
        .endRecord();
    Map<String, String> before = new HashMap<>();
    before.put("name", "lisa");
    before.put("city", "blackpool");
    before.put("dob", "22/09/1992");
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, before);
    Map<String, String> after = new HashMap<>();
    after.put("name", "adam");
    after.put("city", "london");
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, after);
    List<String> expectedData = Lists.newArrayList(
        "1\t{\"name\":\"lisa\",\"city\":\"blackpool\"}\t1",
        "2\t{\"name\":\"adam\",\"city\":\"london\"}\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test
  public void promoteIntToLong() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredInt(EVOLUTION_COLUMN)
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredFloat(EVOLUTION_COLUMN)
        .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 2L);
    List<String> expectedData = Lists.newArrayList(
        "1\t1.0\t1",
        "2\t2.0\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test
  public void promoteIntToFloat() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredInt(EVOLUTION_COLUMN)
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredFloat(EVOLUTION_COLUMN)
        .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 2f);
    List<String> expectedData = Lists.newArrayList(
        "1\t1.0\t1",
        "2\t2.0\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test
  public void promoteIntToDouble() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredInt(EVOLUTION_COLUMN)
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredDouble(EVOLUTION_COLUMN)
        .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 2d);
    List<String> expectedData = Lists.newArrayList(
        "1\t1.0\t1",
        "2\t2.0\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test
  public void promoteFloatToDouble() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredFloat(EVOLUTION_COLUMN)
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredDouble(EVOLUTION_COLUMN)
        .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1f);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 2d);
    List<String> expectedData = Lists.newArrayList(
        "1\t1.0\t1",
        "2\t2.0\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test
  public void promoteLongToFloat() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredLong(EVOLUTION_COLUMN)
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredFloat(EVOLUTION_COLUMN)
        .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1L);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 2f);
    List<String> expectedData = Lists.newArrayList(
        "1\t1.0\t1",
        "2\t2.0\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test
  public void promoteLongToDouble() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredLong(EVOLUTION_COLUMN)
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredDouble(EVOLUTION_COLUMN)
        .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1L);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 2d);
    List<String> expectedData = Lists.newArrayList(
        "1\t1.0\t1",
        "2\t2.0\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test(expected = Exception.class)
  public void demoteLongToInt() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredLong(EVOLUTION_COLUMN)
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredInt(EVOLUTION_COLUMN)
        .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1L);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 2);
    List<String> expectedData = Lists.newArrayList(
        "1\t1\t1",
        "2\t2\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test(expected = Exception.class)
  public void demoteDoubleToFloat() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredLong(EVOLUTION_COLUMN)
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredInt(EVOLUTION_COLUMN)
        .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1d);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 2f);
    List<String> expectedData = Lists.newArrayList(
        "1\t1.0\t1",
        "2\t2.0\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test(expected = Exception.class)
  public void demoteDoubleToInt() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredLong(EVOLUTION_COLUMN)
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredInt(EVOLUTION_COLUMN)
        .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1d);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 2);
    List<String> expectedData = Lists.newArrayList(
        "1\t1\t1",
        "2\t2\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test(expected = Exception.class)
  public void demoteFloatToInt() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredLong(EVOLUTION_COLUMN)
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredInt(EVOLUTION_COLUMN)
        .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1f);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 2);
    List<String> expectedData = Lists.newArrayList(
        "1\t1\t1",
        "2\t2\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test(expected = Exception.class)
  public void demoteDoubleToLong() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredDouble(EVOLUTION_COLUMN)
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredLong(EVOLUTION_COLUMN)
        .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1d);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 2l);
    List<String> expectedData = Lists.newArrayList(
        "1\t1.0\t1",
        "2\t2.0\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test(expected = Exception.class)
  public void demoteFloatToLong() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredFloat(EVOLUTION_COLUMN)
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredLong(EVOLUTION_COLUMN)
        .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1f);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 2l);
    List<String> expectedData = Lists.newArrayList(
        "1\t1.0\t1",
        "2\t2.0\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test
  public void addValueToEnum() throws Exception {
    Schema schema = getSchemaFieldAssembler().name(EVOLUTION_COLUMN)
        .type()
        .enumeration(EVOLUTION_COLUMN + "_enum")
        .symbols("FIRST")
        .noDefault()
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().name(EVOLUTION_COLUMN)
        .type()
        .enumeration(EVOLUTION_COLUMN + "_enum")
        .symbols("FIRST", "SECOND")
        .noDefault()
        .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "FIRST");
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "SECOND");
    List<String> expectedData = Lists.newArrayList(
        "1\tFIRST\t1",
        "2\tSECOND\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  @Test
  public void removeValueFromEnum() throws Exception {
    Schema schema = getSchemaFieldAssembler().name(EVOLUTION_COLUMN)
        .type()
        .enumeration(EVOLUTION_COLUMN + "_enum")
        .symbols("FIRST", "SECOND")
        .noDefault()
        .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().name(EVOLUTION_COLUMN)
        .type()
        .enumeration(EVOLUTION_COLUMN + "_enum")
        .symbols("FIRST")
        .noDefault()
        .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "SECOND");
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "FIRST");
    List<String> expectedData = Lists.newArrayList(
        "1\tSECOND\t1",
        "2\tFIRST\t2"
    );

    runTest(schema, evolvedSchema, beforeEvolution, afterEvolution);
    runDataChecks(evolvedSchema, expectedData);
  }

  private static class FieldDataWrapper {

    private String fieldName;
    private Object expectedData;

    public FieldDataWrapper() {
    }

    public FieldDataWrapper(String fieldName, Object expectedData) {
      this.fieldName = fieldName;
      this.expectedData = expectedData;
    }
  }

  private SchemaBuilder.FieldAssembler<Schema> getSchemaFieldAssembler() {
    return SchemaBuilder
        .builder("name.space")
        .record(PARTITIONED_TABLE)
        .fields()
        .requiredInt("id");
  }

  private void runTest(
      Schema schema,
      Schema evolvedSchema,
      FieldDataWrapper beforeEvolution,
      FieldDataWrapper afterEvolution) throws Exception {
    // Create initial replica table with the original schema (setting a Circus Train event id manually).
    Table replicaTable = helper.createParquetPartitionedTable(
        toUri(replicaWarehouseUri, REPLICA_DB, TABLE),
        REPLICA_DB,
        TABLE,
        schema,
        beforeEvolution.fieldName,
        beforeEvolution.expectedData,
        1);
    log.info(">>>> Table {} ", metaStoreClient.getTable(REPLICA_DB, TABLE));

    replicaTable.getParameters().put("com.hotels.bdp.circustrain.replication.event", "event_id");
    metaStoreClient.alter_table(REPLICA_DB, TABLE, replicaTable);

    // Create the source table with the evolved schema
    Table sourceTable = helper.createParquetPartitionedTable(
        toUri(sourceWarehouseUri, SOURCE_DB, TABLE),
        SOURCE_DB,
        TABLE,
        evolvedSchema,
        afterEvolution.fieldName,
        afterEvolution.expectedData,
        2);
    log.info(">>>> Table {} ", metaStoreClient.getTable(SOURCE_DB, TABLE));

    // Create the original partition (with the original schema) and add to the source table
    URI partition = helper.createData(toUri(sourceWarehouseUri, SOURCE_DB, TABLE),
        schema, Integer.toString(1), 1, beforeEvolution.fieldName, beforeEvolution.expectedData);
    metaStoreClient.add_partitions(Arrays.asList(
        newTablePartition(sourceTable, Arrays.asList("1"), partition)
    ));

    CircusTrainRunner runner = CircusTrainRunner
        .builder(SOURCE_DB, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .replicaDatabaseName(REPLICA_DB)
        .sourceMetaStore(thriftMetaStoreRule.getThriftConnectionUri(), thriftMetaStoreRule.connectionURL(),
            thriftMetaStoreRule.driverClassName())
        .replicaMetaStore(thriftMetaStoreRule.getThriftConnectionUri())
        .build();

    // Set up the asserts
    exit.expectSystemExitWithStatus(0);

    // Do the replication
    File config = dataFolder.getFile("partitioned-single-table-one-partition.yml");
    runner.run(config.getAbsolutePath());
  }

  private void runDataChecks(Schema schema, List<String> expectedData) throws Exception {
    assertTable(thriftMetaStoreRule.newClient(), schema, SOURCE_DB, TABLE, expectedData);
    assertTable(thriftMetaStoreRule.newClient(), schema, REPLICA_DB, TABLE, expectedData);
  }

  private void assertTable(HiveMetaStoreClient client, Schema schema, String database, String table,
      List<String> expectedData) throws Exception {
    assertThat(client.getAllTables(database).size(), is(1));
    Table hiveTable = client.getTable(database, table);
    List<FieldSchema> cols = hiveTable.getSd().getCols();
    assertThat(cols.size(), is(schema.getFields().size()));
    assertColumnSchema(schema, cols);
    PartitionIterator partitionIterator = new PartitionIterator(client, hiveTable, (short) 1000);
    List<Partition> partitions = new ArrayList<>();
    while (partitionIterator.hasNext()) {
      Partition partition = partitionIterator.next();
      assertColumnSchema(schema, partition.getSd().getCols());
      partitions.add(partition);
    }
    assertThat(partitions.size(), is(2));
    List<String> data = shell.executeQuery("select * from " + database + "." + table);
    assertThat(data.size(), is(expectedData.size()));
    assertThat(data.containsAll(expectedData), is(true));
  }

  private void assertColumnSchema(Schema schema, List<FieldSchema> cols) throws SerDeException {
    AvroObjectInspectorGenerator schemaInspector = new AvroObjectInspectorGenerator(schema);
    for (int i = 0; i < cols.size(); i++) {
      assertThat(cols.get(i).getType(), is(schemaInspector.getColumnTypes().get(i).toString()));
      assertThat(cols.get(i).getName(), is(schemaInspector.getColumnNames().get(i)));
    }
  }
}
