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

import static java.sql.DriverManager.getConnection;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static com.hotels.bdp.circustrain.integration.IntegrationTestHelper.DATABASE;
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
import org.apache.thrift.TException;
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

public class CircusTrainSchemaEvolutionIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(CircusTrainSchemaEvolutionIntegrationTest.class);
  private static final String DETAILS_FIELD = "details";

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

  @Test
  public void addFieldAtEnd() throws Exception {
    Schema schema = getSchemaFieldAssembler().endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().requiredString(DETAILS_FIELD)
            .endRecord();
    String fieldType = "string";
    EvolutionHelper afterEvolution = new EvolutionHelper(fieldType, "Max");
    replicate(schema, evolvedSchema,  new EvolutionHelper(), afterEvolution, getAssertion(evolvedSchema, fieldType));
  }

  @Test
  public void removeField() throws Exception {
    Schema schema = getSchemaFieldAssembler().requiredString(DETAILS_FIELD)
            .endRecord();
    Schema evolvedSchema = getSchemaFieldAssembler().endRecord();
    String fieldType = "string";
    EvolutionHelper beforeEvolution = new EvolutionHelper(fieldType, "Max");
    replicate(schema, evolvedSchema,  beforeEvolution, new EvolutionHelper(), getAssertion(evolvedSchema, fieldType));
  }

  private SchemaBuilder.FieldAssembler<Schema> getSchemaFieldAssembler() {
    return SchemaBuilder
            .builder("name.space")
            .record(PARTITIONED_TABLE)
            .fields()
            .requiredInt("id");
  }

  private void replicate(
          Schema schema,
          Schema evolvedSchema,
          EvolutionHelper beforeEvolution,
          EvolutionHelper afterEvolution,
          Assertion assertion) throws Exception {
    // Create initial replica table (setting a Circus Train event id manually).
    Table replicaTable = replicaHelper.createParquetPartitionedTable(
            toUri(replicaWarehouseUri, DATABASE, PARTITIONED_TABLE),
            schema,
            beforeEvolution.fieldType,
            beforeEvolution.data,
            1);
    LOG.info(">>>> Table {} ", replicaCatalog.client().getTable(DATABASE, PARTITIONED_TABLE));

    replicaTable.getParameters().put("com.hotels.bdp.circustrain.replication.event", "event_id");
    replicaCatalog.client().alter_table(DATABASE, PARTITIONED_TABLE, replicaTable);

    // Assert the replica table here?

    // Create the source table with the evolved schema.
    Table table = helper.createParquetPartitionedTable(
            toUri(sourceWarehouseUri, DATABASE, PARTITIONED_TABLE),
            evolvedSchema,
            afterEvolution.fieldType,
            afterEvolution.data,
            2);
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, PARTITIONED_TABLE));

    URI partition = URI.create(toUri(sourceWarehouseUri, DATABASE, PARTITIONED_TABLE) + "/hour=" + 1);
    sourceCatalog.client().add_partitions(Arrays.asList(
            newTablePartition(table, Arrays.asList("1"), partition)
    ));

    // Do the replication
    CircusTrainRunner runner = CircusTrainRunner
            .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
            .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
                    sourceCatalog.driverClassName())
            .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
            .build();

    exit.expectSystemExitWithStatus(0);
    exit.checkAssertionAfterwards(assertion);
    File config = dataFolder.getFile("partitioned-single-table-one-partition.yml");
    runner.run(config.getAbsolutePath());
  }

  private Assertion getAssertion(Schema schema, String fieldType) {
    return () -> {
      assertTable(sourceCatalog, schema, fieldType);
      assertTable(replicaCatalog, schema, fieldType);
    };
  }

  private void assertTable(ThriftHiveMetaStoreJUnitRule catalog, Schema schema, String fieldType) throws TException {
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

  private void assertColumnSchema(Schema schema, List<FieldSchema> cols) {
    for (int i = 0; i < cols.size(); i++) {
      assertThat(cols.get(i).getType(), is(schema.getFields().get(i).schema().getType().toString().toLowerCase()));
      assertThat(cols.get(i).getName(), is(schema.getFields().get(i).name()));
    }
  }

  @Test
  public void renameField() {

  }

  @Test
  public void addAndRemoveDefaultValueFromField() {

  }

  @Test
  public void makeFieldNullable() {

  }

  @Test
  public void makeFieldUnion() {

  }

  @Test
  public void addAndRemoveTypeFromUnion() {

  }

  @Test
  public void promoteIntToLongFloatDouble() {

  }

  private static class EvolutionHelper {
    private String fieldType;
    private Object data;

    public EvolutionHelper() {
    }

    public EvolutionHelper(String fieldType, Object data) {
      this.fieldType = fieldType;
      this.data = data;
    }
  }

}
