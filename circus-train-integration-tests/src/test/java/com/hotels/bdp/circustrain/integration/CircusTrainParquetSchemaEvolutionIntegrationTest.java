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

import static com.hotels.bdp.circustrain.integration.IntegrationTestHelper.DATABASE;
import static com.hotels.bdp.circustrain.integration.IntegrationTestHelper.EVOLUTION_COLUMN;
import static com.hotels.bdp.circustrain.integration.IntegrationTestHelper.PARTITIONED_TABLE;
import static com.hotels.bdp.circustrain.integration.utils.TestUtils.newTablePartition;
import static com.hotels.bdp.circustrain.integration.utils.TestUtils.toUri;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.avro.AvroObjectInspectorGenerator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.Assertion;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fm.last.commons.test.file.ClassDataFolder;
import fm.last.commons.test.file.DataFolder;

import com.hotels.bdp.circustrain.common.test.base.CircusTrainRunner;
import com.hotels.bdp.circustrain.common.test.junit.rules.ServerSocketRule;
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;
import com.hotels.hcommon.hive.metastore.iterator.PartitionIterator;

public class CircusTrainParquetSchemaEvolutionIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(CircusTrainParquetSchemaEvolutionIntegrationTest.class);

  public @Rule ExpectedSystemExit exit = ExpectedSystemExit.none();
  public @Rule TemporaryFolder temporaryFolder = new TemporaryFolder();
  public @Rule DataFolder dataFolder = new ClassDataFolder();
  public @Rule ThriftHiveMetaStoreJUnitRule sourceCatalog = new ThriftHiveMetaStoreJUnitRule(DATABASE);
  public @Rule ThriftHiveMetaStoreJUnitRule replicaCatalog = new ThriftHiveMetaStoreJUnitRule(DATABASE);
  public @Rule ServerSocketRule serverSocketRule = new ServerSocketRule();

  private File sourceWarehouseUri;
  private File replicaWarehouseUri;
  private File housekeepingDbLocation;

  private IntegrationTestHelper helper;
  private IntegrationTestHelper replicaHelper;

  @Before
  public void init() throws Exception {
    sourceWarehouseUri = temporaryFolder.newFolder("source-warehouse");
    replicaWarehouseUri = temporaryFolder.newFolder("replica-warehouse");
    temporaryFolder.newFolder("db");
    housekeepingDbLocation = new File(new File(temporaryFolder.getRoot(), "db"), "housekeeping");

    helper = new IntegrationTestHelper(sourceCatalog.client());
    replicaHelper = new IntegrationTestHelper(replicaCatalog.client());
  }

  private String housekeepingDbJdbcUrl() throws ClassNotFoundException {
    Class.forName("org.h2.Driver");
    String jdbcUrl = "jdbc:h2:" + housekeepingDbLocation.getAbsolutePath() + ";AUTO_SERVER=TRUE;DB_CLOSE_ON_EXIT=FALSE";
    return jdbcUrl;
  }

  // TODO: assert original data in replica table
  // TODO: assert data in evolved replica table

  @Test
  public void addFieldAtEnd() throws Exception {
    Schema schema = getSchemaFieldAssembler().endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredString(EVOLUTION_COLUMN)
            .endRecord();
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "value");
    runTest(schema, evolvedSchema,  new FieldDataWrapper(), afterEvolution, getAssertion(evolvedSchema));
  }

  @Test
  public void removeField() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredString(EVOLUTION_COLUMN)
            .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "value");
    runTest(schema, evolvedSchema,  beforeEvolution, new FieldDataWrapper(), getAssertion(evolvedSchema));
  }

  @Test
  public void renameField() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredString(EVOLUTION_COLUMN)
            .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredString(EVOLUTION_COLUMN + "_renamed")
            .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "value");
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN + "_renamed", "value");
    runTest(schema, evolvedSchema,  beforeEvolution, afterEvolution, getAssertion(evolvedSchema));
  }

  @Test
  public void addDefaultValue() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredString(EVOLUTION_COLUMN)
            .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().nullableString(EVOLUTION_COLUMN, "default")
            .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "value");
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, null);
    runTest(schema, evolvedSchema,  beforeEvolution, afterEvolution, getAssertion(evolvedSchema));
  }

  @Test
  public void removeDefaultValue() throws Exception {
    Schema schema = getSchemaFieldAssembler().nullableString(EVOLUTION_COLUMN, "default")
            .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredString(EVOLUTION_COLUMN)
            .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, null);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "value");
    runTest(schema, evolvedSchema,  beforeEvolution, afterEvolution, getAssertion(evolvedSchema));
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
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "value");
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, null);
    runTest(schema, evolvedSchema,  beforeEvolution, afterEvolution, getAssertion(evolvedSchema));
  }

  @Test
  public void makeFieldUnion() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredString(EVOLUTION_COLUMN)
            .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().name(EVOLUTION_COLUMN)
            .type()
            .unionOf()
            .stringType()
            .and()
            .booleanType()
            .endUnion()
            .noDefault()
            .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "value");
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "value");
    runTest(schema, evolvedSchema,  beforeEvolution, afterEvolution, getAssertion(evolvedSchema));
  }

  @Test
  public void addTypeToUnion() throws Exception {
    Schema schema = getSchemaFieldAssembler().name(EVOLUTION_COLUMN)
            .type()
            .unionOf()
            .stringType()
            .and()
            .booleanType()
            .endUnion()
            .noDefault()
            .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().name(EVOLUTION_COLUMN)
            .type()
            .unionOf()
            .stringType()
            .and()
            .booleanType()
            .and()
            .intType()
            .endUnion()
            .noDefault()
            .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "value");
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "value");
    runTest(schema, evolvedSchema,  beforeEvolution, afterEvolution, getAssertion(evolvedSchema));
  }

  @Test
  public void removeTypeFromUnion() throws Exception {
    Schema schema = getSchemaFieldAssembler().name(EVOLUTION_COLUMN)
            .type()
            .unionOf()
            .stringType()
            .and()
            .booleanType()
            .and()
            .intType()
            .endUnion()
            .noDefault()
            .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().name(EVOLUTION_COLUMN)
            .type()
            .unionOf()
            .stringType()
            .and()
            .booleanType()
            .endUnion()
            .noDefault()
            .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "value");
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, "value");
    runTest(schema, evolvedSchema,  beforeEvolution, afterEvolution, getAssertion(evolvedSchema));
  }

  @Test
  public void promoteIntToLong() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredInt(EVOLUTION_COLUMN)
            .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredFloat(EVOLUTION_COLUMN)
            .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1L);
    runTest(schema, evolvedSchema,  beforeEvolution, afterEvolution, getAssertion(evolvedSchema));
  }

  @Test
  public void promoteIntToFloat() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredInt(EVOLUTION_COLUMN)
            .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredFloat(EVOLUTION_COLUMN)
            .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1f);
    runTest(schema, evolvedSchema,  beforeEvolution, afterEvolution, getAssertion(evolvedSchema));
  }

  @Test
  public void promoteIntToDouble() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredInt(EVOLUTION_COLUMN)
            .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredDouble(EVOLUTION_COLUMN)
            .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1d);
    runTest(schema, evolvedSchema,  beforeEvolution, afterEvolution, getAssertion(evolvedSchema));
  }

  @Test
  public void promoteFloatToDouble() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredFloat(EVOLUTION_COLUMN)
            .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredDouble(EVOLUTION_COLUMN)
            .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1f);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1d);
    runTest(schema, evolvedSchema,  beforeEvolution, afterEvolution, getAssertion(evolvedSchema));
  }

  @Test
  public void promoteLongToFloat() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredLong(EVOLUTION_COLUMN)
            .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredFloat(EVOLUTION_COLUMN)
            .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1L);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1f);
    runTest(schema, evolvedSchema,  beforeEvolution, afterEvolution, getAssertion(evolvedSchema));
  }

  @Test
  public void promoteLongToDouble() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredLong(EVOLUTION_COLUMN)
            .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredDouble(EVOLUTION_COLUMN)
            .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1L);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1d);
    runTest(schema, evolvedSchema,  beforeEvolution, afterEvolution, getAssertion(evolvedSchema));
  }

  @Test
  public void demoteLongToInt() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredLong(EVOLUTION_COLUMN)
            .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredInt(EVOLUTION_COLUMN)
            .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1L);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1);
    runTest(schema, evolvedSchema,  beforeEvolution, afterEvolution, getAssertion(evolvedSchema));
  }

  @Test
  public void demoteDoubleToFloat() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredLong(EVOLUTION_COLUMN)
            .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredInt(EVOLUTION_COLUMN)
            .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1d);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1f);
    runTest(schema, evolvedSchema,  beforeEvolution, afterEvolution, getAssertion(evolvedSchema));
  }

  @Test
  public void demoteDoubleToInt() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredLong(EVOLUTION_COLUMN)
            .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredInt(EVOLUTION_COLUMN)
            .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1d);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1);
    runTest(schema, evolvedSchema,  beforeEvolution, afterEvolution, getAssertion(evolvedSchema));
  }

  @Test
  public void demoteFloatToInt() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredLong(EVOLUTION_COLUMN)
            .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredInt(EVOLUTION_COLUMN)
            .endRecord();
    FieldDataWrapper beforeEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1f);
    FieldDataWrapper afterEvolution = new FieldDataWrapper(EVOLUTION_COLUMN, 1);
    runTest(schema, evolvedSchema,  beforeEvolution, afterEvolution, getAssertion(evolvedSchema));
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
    runTest(schema, evolvedSchema,  beforeEvolution, afterEvolution, getAssertion(evolvedSchema));
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
    runTest(schema, evolvedSchema,  beforeEvolution, afterEvolution, getAssertion(evolvedSchema));
  }

  private static class FieldDataWrapper {
    private String fieldName;
    private Object data;

    public FieldDataWrapper() {
    }

    public FieldDataWrapper(String fieldName, Object data) {
      this.fieldName = fieldName;
      this.data = data;
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
          FieldDataWrapper afterEvolution,
          Assertion assertion) throws Exception {
    // Create initial replica table with the original schema (setting a Circus Train event id manually).
    Table replicaTable = replicaHelper.createParquetPartitionedTable(
            toUri(replicaWarehouseUri, DATABASE, PARTITIONED_TABLE),
            schema,
            beforeEvolution.fieldName,
            beforeEvolution.data,
            1);
    LOG.info(">>>> Table {} ", replicaCatalog.client().getTable(DATABASE, PARTITIONED_TABLE));

    replicaTable.getParameters().put("com.hotels.bdp.circustrain.replication.event", "event_id");
    replicaCatalog.client().alter_table(DATABASE, PARTITIONED_TABLE, replicaTable);

    // Create the source table with the evolved schema
    Table sourceTable = helper.createParquetPartitionedTable(
            toUri(sourceWarehouseUri, DATABASE, PARTITIONED_TABLE),
            evolvedSchema,
            afterEvolution.fieldName,
            afterEvolution.data,
            2);
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, PARTITIONED_TABLE));

    // Create the original partition (with the original schema) and add to the source table
    URI partition = helper.createData(toUri(sourceWarehouseUri, DATABASE, PARTITIONED_TABLE),
            schema, Integer.toString(1), 1, beforeEvolution.fieldName, beforeEvolution.data);
    sourceCatalog.client().add_partitions(Arrays.asList(
            newTablePartition(sourceTable, Arrays.asList("1"), partition)
    ));

    CircusTrainRunner runner = CircusTrainRunner
            .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
            .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
                    sourceCatalog.driverClassName())
            .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
            .build();

    // Set up the asserts
    exit.expectSystemExitWithStatus(0);
    exit.checkAssertionAfterwards(assertion);

    // Do the replication
    File config = dataFolder.getFile("partitioned-single-table-one-partition.yml");
    runner.run(config.getAbsolutePath());
  }

  private Assertion getAssertion(Schema schema) {
    return () -> {
      assertTable(sourceCatalog, schema);
      assertTable(replicaCatalog, schema);
    };
  }

  private void assertTable(ThriftHiveMetaStoreJUnitRule catalog, Schema schema) throws Exception {
    assertThat(catalog.client().getAllTables(DATABASE).size(), is(1));
    Table table = catalog.client().getTable(DATABASE, PARTITIONED_TABLE);
    List<FieldSchema> cols = table.getSd().getCols();
    assertThat(cols.size(), is(schema.getFields().size()));
    assertColumnSchema(schema, cols);
    PartitionIterator partitionIterator = new PartitionIterator(catalog.client(), table, (short) 1000);
    List<Partition> partitions = new ArrayList<>();
    while (partitionIterator.hasNext()) {
      Partition partition = partitionIterator.next();
      assertColumnSchema(schema, partition.getSd().getCols());
      partitions.add(partition);
    }
    assertThat(partitions.size(), is(2));
  }

  private void assertColumnSchema(Schema schema, List<FieldSchema> cols) throws SerDeException {
    AvroObjectInspectorGenerator schemaInspector = new AvroObjectInspectorGenerator(schema);
    for (int i = 0; i < cols.size(); i++) {
      assertThat(cols.get(i).getType(), is(schemaInspector.getColumnTypes().get(i).toString()));
      assertThat(cols.get(i).getName(), is(schemaInspector.getColumnNames().get(i)));
    }
  }
}
