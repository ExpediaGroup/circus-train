/**
 * Copyright (C) 2016-2018 Expedia Inc.
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

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.isExternalTable;
import static org.apache.hadoop.hive.metastore.MetaStoreUtils.isView;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.REPLICATION_EVENT;
import static com.hotels.bdp.circustrain.integration.IntegrationTestHelper.DATABASE;
import static com.hotels.bdp.circustrain.integration.IntegrationTestHelper.SOURCE_ENCODED_TABLE;
import static com.hotels.bdp.circustrain.integration.IntegrationTestHelper.SOURCE_MANAGED_PARTITIONED_TABLE;
import static com.hotels.bdp.circustrain.integration.IntegrationTestHelper.SOURCE_MANAGED_UNPARTITIONED_TABLE;
import static com.hotels.bdp.circustrain.integration.IntegrationTestHelper.SOURCE_PARTITIONED_TABLE;
import static com.hotels.bdp.circustrain.integration.IntegrationTestHelper.SOURCE_PARTITIONED_VIEW;
import static com.hotels.bdp.circustrain.integration.IntegrationTestHelper.SOURCE_UNPARTITIONED_TABLE;
import static com.hotels.bdp.circustrain.integration.IntegrationTestHelper.SOURCE_UNPARTITIONED_VIEW;
import static com.hotels.bdp.circustrain.integration.utils.TestUtils.DATA_COLUMNS;
import static com.hotels.bdp.circustrain.integration.utils.TestUtils.HOUSEKEEPING_DB_PASSWD;
import static com.hotels.bdp.circustrain.integration.utils.TestUtils.HOUSEKEEPING_DB_USER;
import static com.hotels.bdp.circustrain.integration.utils.TestUtils.getCleanUpPaths;
import static com.hotels.bdp.circustrain.integration.utils.TestUtils.newTablePartition;
import static com.hotels.bdp.circustrain.integration.utils.TestUtils.toUri;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.Assertion;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.contrib.java.lang.system.internal.CheckExitCalled;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fm.last.commons.test.file.ClassDataFolder;
import fm.last.commons.test.file.DataFolder;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;

import com.hotels.bdp.circustrain.common.test.base.CircusTrainRunner;
import com.hotels.bdp.circustrain.common.test.junit.rules.ServerSocketRule;
import com.hotels.bdp.circustrain.integration.utils.TestUtils;
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;
import com.hotels.housekeeping.model.LegacyReplicaPath;

public class CircusTrainHdfsHdfsIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(CircusTrainHdfsHdfsIntegrationTest.class);

  private static final String TARGET_UNPARTITIONED_TABLE = "ct_table_u_copy";
  private static final String TARGET_PARTITIONED_TABLE = "ct_table_p_copy";
  private static final String TARGET_UNPARTITIONED_MANAGED_TABLE = "ct_table_u_managed_copy";
  private static final String TARGET_PARTITIONED_MANAGED_TABLE = "ct_table_p_managed_copy";
  private static final String TARGET_PARTITIONED_VIEW = "ct_view_p_copy";
  private static final String TARGET_UNPARTITIONED_VIEW = "ct_view_u_copy";

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
  public void housekeepingOnly() throws Exception {
    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("housekeeping-only.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .graphiteUri("localhost:" + serverSocketRule.port())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        List<String> completionCodeMetrics = FluentIterable
            .from(Splitter.on('\n').split(new String(serverSocketRule.getOutput())))
            .filter(new Predicate<String>() {
              @Override
              public boolean apply(String input) {
                return input.contains("completion_code") || input.contains("housekeeping");
              }
            })
            .transform(new Function<String, String>() {
              @Override
              public String apply(String input) {
                return input.substring(0, input.lastIndexOf(" "));
              }
            })
            .toList();

        assertThat(completionCodeMetrics.size(), is(1));
        assertThat(completionCodeMetrics.contains("dummy.test.housekeeping 1"), is(true));

        String jdbcUrl = housekeepingDbJdbcUrl();
        try (Connection conn = getConnection(jdbcUrl, HOUSEKEEPING_DB_USER, HOUSEKEEPING_DB_PASSWD)) {
          List<LegacyReplicaPath> cleanUpPaths = TestUtils
              .getCleanUpPaths(conn, "SELECT * FROM circus_train.legacy_replica_path");
          assertThat(cleanUpPaths.size(), is(0));
          try {
            getCleanUpPaths(conn, "SELECT * FROM circus_train.legacy_replica_path_aud");
          } catch (SQLException e) {
            assertThat(e.getMessage().startsWith("Table \"LEGACY_REPLICA_PATH_AUD\" not found;"), is(true));
          }
        }
      }
    });
    runner.run(config.getAbsolutePath(), "housekeeping");
  }

  @Test
  public void twoTablesWithGraphiteNoHousekeeping() throws Exception {
    helper.createPartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_PARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_PARTITIONED_TABLE));

    helper.createUnpartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_UNPARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_UNPARTITIONED_TABLE));

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("mixed-two-tables-with-graphite-no-housekeeping.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .graphiteUri("localhost:" + serverSocketRule.port())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        List<String> completionCodeMetrics = FluentIterable
            .from(Splitter.on('\n').split(new String(serverSocketRule.getOutput())))
            .filter(new Predicate<String>() {
              @Override
              public boolean apply(String input) {
                return input.contains("completion_code") || input.contains("housekeeping");
              }
            })
            .transform(new Function<String, String>() {
              @Override
              public String apply(String input) {
                return input.substring(0, input.lastIndexOf(" "));
              }
            })
            .toList();

        assertThat(completionCodeMetrics.size(), is(3));
        assertThat(completionCodeMetrics.contains("dummy.test.ct_database.ct_table_p.completion_code 1"), is(true));
        assertThat(completionCodeMetrics.contains("dummy.test.ct_database.ct_table_u.completion_code 1"), is(true));
        assertThat(completionCodeMetrics.contains("dummy.test.completion_code 1"), is(true));
      }
    });
    runner.run(config.getAbsolutePath(), "replication");
  }

  @Test
  public void partitionedTableHousekeepingEnabledNoAudit() throws Exception {
    helper.createPartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_PARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_PARTITIONED_TABLE));

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("partitioned-single-table-no-housekeeping.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        String jdbcUrl = housekeepingDbJdbcUrl();
        try (Connection conn = getConnection(jdbcUrl, HOUSEKEEPING_DB_USER, HOUSEKEEPING_DB_PASSWD)) {
          List<LegacyReplicaPath> cleanUpPaths = TestUtils
              .getCleanUpPaths(conn, "SELECT * FROM circus_train.legacy_replica_path");
          assertThat(cleanUpPaths.size(), is(0));
          try {
            getCleanUpPaths(conn, "SELECT * FROM circus_train.legacy_replica_path_aud");
          } catch (SQLException e) {
            assertThat(e.getMessage().startsWith("Table \"LEGACY_REPLICA_PATH_AUD\" not found;"), is(true));
          }
        }
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void partitionedTableHousekeepingEnabledWithAudit() throws Exception {
    helper.createPartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_PARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_PARTITIONED_TABLE));

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("partitioned-single-table-with-housekeeping.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();

    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        // Assert deleted path
        String jdbcUrl = housekeepingDbJdbcUrl();
        try (Connection conn = DriverManager.getConnection(jdbcUrl, HOUSEKEEPING_DB_USER, HOUSEKEEPING_DB_PASSWD)) {
          List<LegacyReplicaPath> cleanUpPaths = getCleanUpPaths(conn,
              "SELECT * FROM circus_train.legacy_replica_path");
          assertThat(cleanUpPaths.size(), is(0));
          List<LegacyReplicaPath> cleanUpPathsAudit = getCleanUpPaths(conn,
              "SELECT * FROM circus_train.legacy_replica_path_aud");
          assertThat(cleanUpPathsAudit.size(), is(1));
          assertThat(cleanUpPathsAudit.get(0).getEventId(), is("event-124"));
          assertThat(cleanUpPathsAudit.get(0).getPathEventId(), is("event-123"));
          assertThat(cleanUpPathsAudit.get(0).getPath(), is("file:/foo/bar/event-123/deleteme"));
        }
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void unpartitionedTableHousekeepingEnabledWithAudit() throws Exception {
    helper.createUnpartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_UNPARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_UNPARTITIONED_TABLE));

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("unpartitioned-single-table-with-housekeeping.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        // Assert deleted path
        String jdbcUrl = housekeepingDbJdbcUrl();
        try (Connection conn = DriverManager.getConnection(jdbcUrl, HOUSEKEEPING_DB_USER, HOUSEKEEPING_DB_PASSWD)) {
          List<LegacyReplicaPath> cleanUpPaths = getCleanUpPaths(conn,
              "SELECT * FROM circus_train.legacy_replica_path");
          assertThat(cleanUpPaths.size(), is(0));
          List<LegacyReplicaPath> cleanUpPathsAudit = getCleanUpPaths(conn,
              "SELECT * FROM circus_train.legacy_replica_path_aud");
          assertThat(cleanUpPathsAudit.size(), is(1));
          assertThat(cleanUpPathsAudit.get(0).getEventId(), is("event-124"));
          assertThat(cleanUpPathsAudit.get(0).getPathEventId(), is("event-123"));
          assertThat(cleanUpPathsAudit.get(0).getPath(), is("file:/foo/bar/event-123/deleteme"));
        }
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void unpartitionedTableHousekeepingEnabledNoAudit() throws Exception {
    helper.createUnpartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_UNPARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_UNPARTITIONED_TABLE));

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("unpartitioned-single-table-no-housekeeping.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        // Assert deleted path
        String jdbcUrl = housekeepingDbJdbcUrl();
        try (Connection conn = DriverManager.getConnection(jdbcUrl, HOUSEKEEPING_DB_USER, HOUSEKEEPING_DB_PASSWD)) {
          List<LegacyReplicaPath> cleanUpPaths = getCleanUpPaths(conn,
              "SELECT * FROM circus_train.legacy_replica_path");
          assertThat(cleanUpPaths.size(), is(0));
          try {
            getCleanUpPaths(conn, "SELECT * FROM circus_train.legacy_replica_path_aud");
          } catch (SQLException e) {
            assertThat(e.getMessage().startsWith("Table \"LEGACY_REPLICA_PATH_AUD\" not found;"), is(true));
          }
        }
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void deleteUnpartitionedTable() throws Exception {
    // create table in replica metastore
    replicaHelper.createUnpartitionedTable(toUri(replicaWarehouseUri, DATABASE, SOURCE_UNPARTITIONED_TABLE));
    Table table = replicaHelper.alterTableSetCircusTrainParameter(DATABASE, SOURCE_UNPARTITIONED_TABLE);
    final String replicaPath = table.getSd().getLocation();
    LOG.info(">>>> Table {} ", table);

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("destructive-unpartitioned-single-table.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        assertThat(replicaCatalog.client().tableExists(DATABASE, SOURCE_UNPARTITIONED_TABLE), is(false));
        // Assert deleted path
        String jdbcUrl = housekeepingDbJdbcUrl();
        try (Connection conn = DriverManager.getConnection(jdbcUrl, HOUSEKEEPING_DB_USER, HOUSEKEEPING_DB_PASSWD)) {
          List<LegacyReplicaPath> cleanUpPaths = getCleanUpPaths(conn,
              "SELECT * FROM circus_train.legacy_replica_path");
          assertThat(cleanUpPaths.size(), is(1));
          assertThat(cleanUpPaths.get(0).getPath(), is(replicaPath));
        }
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void deletePartitionInPartitionedTable() throws Exception {
    helper.createPartitionedTable(toUri(replicaWarehouseUri, DATABASE, SOURCE_PARTITIONED_TABLE));
    // create table + partitions in replica metastore
    replicaHelper.createPartitionedTable(toUri(replicaWarehouseUri, DATABASE, SOURCE_PARTITIONED_TABLE));
    Table table = replicaHelper.alterTableSetCircusTrainParameter(DATABASE, SOURCE_PARTITIONED_TABLE);
    LOG.info(">>>> Table {} ", table);
    List<Partition> partitions = replicaCatalog.client().listPartitions(DATABASE, SOURCE_PARTITIONED_TABLE, (short) 10);
    Partition partition = partitions.get(0);
    final String partitionLocation = partition.getSd().getLocation();

    // drop a partition in source;
    sourceCatalog
        .client()
        .dropPartition(DATABASE, SOURCE_PARTITIONED_TABLE,
            Warehouse.makePartName(table.getPartitionKeys(), partition.getValues()), false);

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("destructive-partitioned-single-table.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        assertThat(replicaCatalog.client().tableExists(DATABASE, SOURCE_PARTITIONED_TABLE), is(true));
        List<Partition> partitions = replicaCatalog
            .client()
            .listPartitions(DATABASE, SOURCE_PARTITIONED_TABLE, (short) 10);

        // TODO PD need to look at why we get more partitions still something is not working
        assertThat(partitions.size(), is(1));
        // Assert deleted path
        String jdbcUrl = housekeepingDbJdbcUrl();
        try (Connection conn = DriverManager.getConnection(jdbcUrl, HOUSEKEEPING_DB_USER, HOUSEKEEPING_DB_PASSWD)) {
          List<LegacyReplicaPath> cleanUpPaths = getCleanUpPaths(conn,
              "SELECT * FROM circus_train.legacy_replica_path");
          assertThat(cleanUpPaths.size(), is(1));
          assertThat(cleanUpPaths.get(0).getPath(), is(partitionLocation));
        }
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void unpartitionedTableMetadataMirror() throws Exception {
    helper.createManagedUnpartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE));

    // adjusting the sourceTable, mimicking the change we want to update
    Table sourceTable = sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE);
    sourceTable.putToParameters("paramToUpdate", "updated");
    sourceCatalog.client().alter_table(sourceTable.getDbName(), sourceTable.getTableName(), sourceTable);

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("unpartitioned-single-table-mirror.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        Table hiveTable = replicaCatalog.client().getTable(DATABASE, TARGET_UNPARTITIONED_MANAGED_TABLE);
        assertThat(hiveTable.getDbName(), is(DATABASE));
        assertThat(hiveTable.getTableName(), is(TARGET_UNPARTITIONED_MANAGED_TABLE));
        // MIRRORED table should be set to EXTERNAL
        assertThat(isExternalTable(hiveTable), is(true));
        assertThat(hiveTable.getParameters().get("paramToUpdate"), is("updated"));
        assertThat(hiveTable.getSd().getCols(), is(DATA_COLUMNS));

        File sameAsSourceLocation = new File(sourceWarehouseUri, DATABASE + "/" + SOURCE_MANAGED_UNPARTITIONED_TABLE);
        assertThat(hiveTable.getSd().getLocation() + "/", is(sameAsSourceLocation.toURI().toString()));
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void partitionedTableMetadataMirror() throws Exception {
    helper.createManagedPartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE));

    // adjusting the sourceTable, mimicking the change we want to update
    Table sourceTable = sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE);
    sourceTable.putToParameters("paramToUpdate", "updated");
    sourceCatalog.client().alter_table(sourceTable.getDbName(), sourceTable.getTableName(), sourceTable);
    Partition partition = sourceCatalog
        .client()
        .getPartition(DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE, "continent=Asia/country=China");
    partition.putToParameters("partition_paramToUpdate", "updated");
    sourceCatalog.client().alter_partition(DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE, partition);

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("partitioned-single-table-mirror.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();

    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {

        Table hiveTable = replicaCatalog.client().getTable(DATABASE, TARGET_PARTITIONED_MANAGED_TABLE);
        assertThat(hiveTable.getDbName(), is(DATABASE));
        assertThat(hiveTable.getTableName(), is(TARGET_PARTITIONED_MANAGED_TABLE));
        // MIRRORED table should be set to EXTERNAL
        assertThat(isExternalTable(hiveTable), is(true));
        assertThat(hiveTable.getSd().getCols(), is(DATA_COLUMNS));
        assertThat(hiveTable.getParameters().get("paramToUpdate"), is("updated"));

        File sameAsSourceLocation = new File(sourceWarehouseUri, DATABASE + "/" + SOURCE_MANAGED_PARTITIONED_TABLE);
        assertThat(hiveTable.getSd().getLocation() + "/", is(sameAsSourceLocation.toURI().toString()));

        List<Partition> listPartitions = replicaCatalog
            .client()
            .listPartitions(DATABASE, TARGET_PARTITIONED_MANAGED_TABLE, (short) 50);
        assertThat(listPartitions.size(), is(2));
        assertThat(listPartitions.get(0).getSd().getLocation(),
            is(sameAsSourceLocation.toURI().toString() + "continent=Asia/country=China"));
        assertThat(listPartitions.get(0).getParameters().get("partition_paramToUpdate"), is("updated"));
        assertThat(listPartitions.get(1).getSd().getLocation(),
            is(sameAsSourceLocation.toURI().toString() + "continent=Europe/country=UK"));
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void unpartitionedTableMetadataUpdate() throws Exception {
    helper.createManagedUnpartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE));

    // creating replicaTable
    final URI replicaLocation = toUri(replicaWarehouseUri, DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE);
    TestUtils
        .createUnpartitionedTable(replicaCatalog.client(), DATABASE, TARGET_UNPARTITIONED_MANAGED_TABLE,
            replicaLocation);
    Table table = replicaCatalog.client().getTable(DATABASE, TARGET_UNPARTITIONED_MANAGED_TABLE);
    table.putToParameters(REPLICATION_EVENT.parameterName(), "dummyEventID");
    replicaCatalog.client().alter_table(table.getDbName(), table.getTableName(), table);

    // adjusting the sourceTable, mimicking the change we want to update
    Table sourceTable = sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE);
    sourceTable.putToParameters("paramToUpdate", "updated");
    sourceCatalog.client().alter_table(sourceTable.getDbName(), sourceTable.getTableName(), sourceTable);

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("unpartitioned-single-table-metadata-update.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        Table hiveTable = replicaCatalog.client().getTable(DATABASE, TARGET_UNPARTITIONED_MANAGED_TABLE);
        assertThat(hiveTable.getDbName(), is(DATABASE));
        assertThat(hiveTable.getTableName(), is(TARGET_UNPARTITIONED_MANAGED_TABLE));
        // dummyEventID should be overridden
        assertThat(hiveTable.getParameters().get(REPLICATION_EVENT.parameterName()), startsWith("ctt-"));
        assertThat(hiveTable.getParameters().get("paramToUpdate"), is("updated"));
        assertThat(isExternalTable(hiveTable), is(true));
        assertThat(hiveTable.getSd().getCols(), is(DATA_COLUMNS));
        assertThat(hiveTable.getSd().getLocation(), is(replicaLocation.toString()));
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void partitionedTableMetadataUpdate() throws Exception {
    helper.createManagedPartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE));

    // creating replicaTable
    final URI replicaLocation = toUri(replicaWarehouseUri, DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE);
    TestUtils
        .createPartitionedTable(replicaCatalog.client(), DATABASE, TARGET_PARTITIONED_MANAGED_TABLE, replicaLocation);
    Table table = replicaCatalog.client().getTable(DATABASE, TARGET_PARTITIONED_MANAGED_TABLE);
    table.putToParameters(REPLICATION_EVENT.parameterName(), "dummyEventID");
    URI partitionAsia = URI.create(replicaLocation + "/dummyEventID/continent=Asia");
    final URI partitionChina = URI.create(partitionAsia + "/country=China");
    replicaCatalog
        .client()
        .add_partitions(Arrays.asList(newTablePartition(table, Arrays.asList("Asia", "China"), partitionChina)));
    replicaCatalog.client().alter_table(table.getDbName(), table.getTableName(), table);

    // adjusting the sourceTable, mimicking the change we want to update
    Table sourceTable = sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE);
    sourceTable.putToParameters("paramToUpdate", "updated");
    sourceCatalog.client().alter_table(sourceTable.getDbName(), sourceTable.getTableName(), sourceTable);
    Partition partition = sourceCatalog
        .client()
        .getPartition(DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE, "continent=Asia/country=China");
    partition.putToParameters("partition_paramToUpdate", "updated");
    sourceCatalog.client().alter_partition(DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE, partition);

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("partitioned-single-table-metadata-update.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        Table hiveTable = replicaCatalog.client().getTable(DATABASE, TARGET_PARTITIONED_MANAGED_TABLE);
        assertThat(hiveTable.getDbName(), is(DATABASE));
        assertThat(hiveTable.getTableName(), is(TARGET_PARTITIONED_MANAGED_TABLE));
        // dummyEventID should be overridden
        assertThat(hiveTable.getParameters().get(REPLICATION_EVENT.parameterName()), startsWith("ctp-"));
        assertThat(hiveTable.getParameters().get("paramToUpdate"), is("updated"));
        assertThat(isExternalTable(hiveTable), is(true));
        assertThat(hiveTable.getSd().getCols(), is(DATA_COLUMNS));

        assertThat(hiveTable.getSd().getLocation(), is(replicaLocation.toString()));
        List<Partition> listPartitions = replicaCatalog
            .client()
            .listPartitions(DATABASE, TARGET_PARTITIONED_MANAGED_TABLE, (short) 50);
        assertThat(listPartitions.size(), is(1));
        // Only previously replicated partitions are updated, no NEW partitions are created
        assertThat(listPartitions.get(0).getSd().getLocation(), is(partitionChina.toString()));
        assertThat(listPartitions.get(0).getParameters().get("partition_paramToUpdate"), is("updated"));
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void replicaInSourceMetastore() throws Exception {
    helper.createUnpartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_UNPARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_UNPARTITIONED_TABLE));

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("unpartitioned-single-table-same-metastore.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(sourceCatalog.getThriftConnectionUri()) // Override only this value
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        Table hiveTable = sourceCatalog.client().getTable(DATABASE, TARGET_UNPARTITIONED_TABLE);
        assertThat(hiveTable.getDbName(), is(DATABASE));
        assertThat(hiveTable.getTableName(), is(TARGET_UNPARTITIONED_TABLE));
        assertThat(isExternalTable(hiveTable), is(true));
        assertThat(hiveTable.getSd().getCols(), is(DATA_COLUMNS));
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void partitionedTablePartitionFilterGenerationEnabled() throws Exception {
    helper.createPartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_PARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_PARTITIONED_TABLE));

    exit.expectSystemExitWithStatus(0);
    final File config = dataFolder.getFile("partitioned-single-table-partition-filter-generation-enabled.yml");
    final CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        Table hiveTable = replicaCatalog.client().getTable(DATABASE, TARGET_PARTITIONED_TABLE);
        assertThat(hiveTable.getDbName(), is(DATABASE));
        assertThat(hiveTable.getTableName(), is(TARGET_PARTITIONED_TABLE));
        assertThat(isExternalTable(hiveTable), is(true));
        assertThat(hiveTable.getSd().getCols(), is(DATA_COLUMNS));
        List<Partition> partitions = replicaCatalog
            .client()
            .listPartitions(DATABASE, TARGET_PARTITIONED_TABLE, (short) 10);
        assertThat(partitions.size(), is(2));

        // We kick of another run, the filter generation should find no differences since everything was replicated in
        // the first run so the partitions should be the same.
        try {
          runner.run(config.getAbsolutePath());
          fail(
              "ExpectedSystemExit should throw CheckExitCalled this in the checkAssertionAfterwards() phase so this is expected.");
        } catch (CheckExitCalled expected) {
          assertThat(expected.getStatus(), is(0));
          List<Partition> partitionsAfterSecondRun = replicaCatalog
              .client()
              .listPartitions(DATABASE, TARGET_PARTITIONED_TABLE, (short) 10);
          // If this fails it is most likely the partition location and the second run replicating the data again
          // (generating a new CT-event-id in the path). Root cause is probably the filter generation finding
          // differences and doing an unnecessary replication
          assertThat(partitions, is(partitionsAfterSecondRun));
        }
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void partitionedTableUrlEncodedPaths() throws Exception {
    helper.createTableWithEncodedPartition(toUri(sourceWarehouseUri, DATABASE, SOURCE_ENCODED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_ENCODED_TABLE));

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("partitioned-single-table-no-housekeeping-url-encoded-paths.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        Table hiveTable = replicaCatalog.client().getTable(DATABASE, SOURCE_ENCODED_TABLE);
        assertThat(hiveTable.getDbName(), is(DATABASE));
        assertThat(hiveTable.getTableName(), is(SOURCE_ENCODED_TABLE));
        List<Partition> partitions = replicaCatalog.client().listPartitions(DATABASE, SOURCE_ENCODED_TABLE, (short) 10);
        assertThat(partitions.size(), is(1));
        assertThat(partitions.get(0).getSd().getLocation(), endsWith("continent=Europe/country=U%25K"));
        assertThat(partitions.get(0).getSd().getLocation(), startsWith(replicaWarehouseUri.toURI().toString()));
        Path copiedPartition = new Path(partitions.get(0).getSd().getLocation());
        assertTrue(FileSystem.get(replicaCatalog.conf()).exists(copiedPartition));
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void partitionedTableHousekeepingEnabledNoAuditPartialReplication() throws Exception {
    helper.createPartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_PARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_PARTITIONED_TABLE));

    exit.expectSystemExitWithStatus(-2);
    File config = dataFolder.getFile("partitioned-single-table-no-housekeeping-partial-replication.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        String jdbcUrl = housekeepingDbJdbcUrl();
        try (Connection conn = getConnection(jdbcUrl, HOUSEKEEPING_DB_USER, HOUSEKEEPING_DB_PASSWD)) {
          List<LegacyReplicaPath> cleanUpPaths = TestUtils
              .getCleanUpPaths(conn, "SELECT * FROM circus_train.legacy_replica_path");
          assertThat(cleanUpPaths.size(), is(0));
          try {
            getCleanUpPaths(conn, "SELECT * FROM circus_train.legacy_replica_path_aud");
          } catch (SQLException e) {
            assertThat(e.getMessage().startsWith("Table \"LEGACY_REPLICA_PATH_AUD\" not found;"), is(true));
          }
        }
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void unpartitionedTableReplicateAvroSchema() throws Exception {
    helper.createManagedUnpartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE));

    java.nio.file.Path sourceAvroSchemaPath = Paths.get(sourceWarehouseUri.toString() + "/avro-schema-file.test");
    Files.createDirectories(sourceAvroSchemaPath);

    String avroSchemaBaseUrl = sourceAvroSchemaPath.toString();

    Table sourceTable = sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE);
    sourceTable.putToParameters("avro.schema.url", avroSchemaBaseUrl);
    sourceCatalog.client().alter_table(sourceTable.getDbName(), sourceTable.getTableName(), sourceTable);

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("unpartitioned-single-table-avro-schema.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();

    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        Table replicaHiveTable = replicaCatalog.client().getTable(DATABASE, TARGET_UNPARTITIONED_MANAGED_TABLE);
        String expectedReplicaSchemaUrl = replicaWarehouseUri.toURI().toString() + "ct_database/";
        String transformedAvroUrl = replicaHiveTable.getParameters().get("avro.schema.url");
        assertThat(transformedAvroUrl, startsWith(expectedReplicaSchemaUrl));
      }
    });

    runner.run(config.getAbsolutePath());
  }

  @Test
  public void unpartitionedTableReplicateAvroSchemaOverride() throws Exception {
    helper.createManagedUnpartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE));

    java.nio.file.Path sourceAvroSchemaPath = Paths.get(sourceWarehouseUri.toString() + "/avro-schema-file.test");
    Files.createDirectories(sourceAvroSchemaPath);

    String avroSchemaBaseUrl = sourceAvroSchemaPath.toString();

    Table sourceTable = sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE);
    sourceTable.putToParameters("avro.schema.url", avroSchemaBaseUrl);
    sourceCatalog.client().alter_table(sourceTable.getDbName(), sourceTable.getTableName(), sourceTable);

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("unpartitioned-single-table-avro-schema-override.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();

    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        Table replicaHiveTable = replicaCatalog.client().getTable(DATABASE, TARGET_UNPARTITIONED_MANAGED_TABLE);
        String expectedReplicaSchemaUrl = replicaWarehouseUri.toURI().toString() + "ct_database-override/";
        String transformedAvroUrl = replicaHiveTable.getParameters().get("avro.schema.url");
        assertThat(transformedAvroUrl, startsWith(expectedReplicaSchemaUrl));
      }
    });

    runner.run(config.getAbsolutePath());
  }

  @Test
  public void unpartitionedTableMetadataUpdateAvroSchema() throws Exception {
    helper.createManagedUnpartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE));

    String avroParameter = "avro.schema.url";
    java.nio.file.Path sourceAvroSchemaUploadPath = Paths.get(sourceWarehouseUri.toString() + "/avro-schema-file.test");
    Files.createDirectories(sourceAvroSchemaUploadPath);
    String avroSchemaBaseUrl = sourceAvroSchemaUploadPath.toString();

    URI replicaLocation = toUri(replicaWarehouseUri, DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE);
    TestUtils
        .createUnpartitionedTable(replicaCatalog.client(), DATABASE, TARGET_UNPARTITIONED_MANAGED_TABLE,
            replicaLocation);
    Table table = replicaCatalog.client().getTable(DATABASE, TARGET_UNPARTITIONED_MANAGED_TABLE);
    table.putToParameters(REPLICATION_EVENT.parameterName(), "dummyEventID");
    replicaCatalog.client().alter_table(table.getDbName(), table.getTableName(), table);

    Table sourceTable = sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE);
    sourceTable.putToParameters(avroParameter, avroSchemaBaseUrl);
    sourceCatalog.client().alter_table(sourceTable.getDbName(), sourceTable.getTableName(), sourceTable);

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("unpartitioned-single-table-avro-schema-metadata-update.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        Table replicaHiveTable = replicaCatalog.client().getTable(DATABASE, TARGET_UNPARTITIONED_MANAGED_TABLE);
        String expectedReplicaSchemaUrl = replicaWarehouseUri.toURI().toString() + "ct_database/";
        String transformedAvroUrl = replicaHiveTable.getParameters().get("avro.schema.url");
        assertThat(transformedAvroUrl, startsWith(expectedReplicaSchemaUrl));
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void partitionedTableMetadataUpdateAvroSchema() throws Exception {
    helper.createManagedPartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE));

    java.nio.file.Path sourceAvroSchemaUploadPath = Paths.get(sourceWarehouseUri.toString() + "/avro-schema-file.test");
    Files.createDirectories(sourceAvroSchemaUploadPath);
    String avroSchemaBaseUrl = sourceAvroSchemaUploadPath.toString();

    URI replicaLocation = toUri(replicaWarehouseUri, DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE);
    TestUtils
        .createPartitionedTable(replicaCatalog.client(), DATABASE, TARGET_PARTITIONED_MANAGED_TABLE, replicaLocation);
    Table table = replicaCatalog.client().getTable(DATABASE, TARGET_PARTITIONED_MANAGED_TABLE);
    table.putToParameters(REPLICATION_EVENT.parameterName(), "dummyEventID");

    URI partitionAsia = URI.create(replicaLocation + "/dummyEventID/continent=Asia");
    URI partitionChina = URI.create(partitionAsia + "/country=China");
    replicaCatalog
        .client()
        .add_partitions(Arrays.asList(newTablePartition(table, Arrays.asList("Asia", "China"), partitionChina)));
    replicaCatalog.client().alter_table(table.getDbName(), table.getTableName(), table);

    Table sourceTable = sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE);
    sourceTable.putToParameters("avro.schema.url", avroSchemaBaseUrl);

    sourceCatalog.client().alter_table(sourceTable.getDbName(), sourceTable.getTableName(), sourceTable);
    Partition partition = sourceCatalog
        .client()
        .getPartition(DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE, "continent=Asia/country=China");
    partition.putToParameters("avro.schema.url", avroSchemaBaseUrl);

    sourceCatalog.client().alter_partition(DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE, partition);

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("partitioned-single-table-avro-schema-metadata-update.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        Table replicaHiveTable = replicaCatalog.client().getTable(DATABASE, TARGET_PARTITIONED_MANAGED_TABLE);
        String expectedReplicaSchemaUrl = replicaWarehouseUri.toURI().toString() + "ct_database/";
        String transformedAvroUrl = replicaHiveTable.getParameters().get("avro.schema.url");
        assertThat(transformedAvroUrl, startsWith(expectedReplicaSchemaUrl));

        List<Partition> listPartitions = replicaCatalog
            .client()
            .listPartitions(DATABASE, TARGET_PARTITIONED_MANAGED_TABLE, (short) 50);
        transformedAvroUrl = listPartitions.get(0).getParameters().get("avro.schema.url");
        assertThat(transformedAvroUrl, startsWith(expectedReplicaSchemaUrl));
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void multipleTransformationsApplied() throws Exception {
    helper.createManagedUnpartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE));

    java.nio.file.Path sourceAvroSchemaPath = Paths.get(sourceWarehouseUri.toString() + "/avro-schema-file.test");
    Files.createDirectories(sourceAvroSchemaPath);

    String avroSchemaBaseUrl = sourceAvroSchemaPath.toString();

    Table sourceTable = sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE);
    sourceTable.putToParameters("avro.schema.url", avroSchemaBaseUrl);
    sourceTable.putToParameters("circus.train.test.transformation", "enabled");
    sourceCatalog.client().alter_table(sourceTable.getDbName(), sourceTable.getTableName(), sourceTable);

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("unpartitioned-single-table-avro-schema.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();

    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        Table replicaHiveTable = replicaCatalog.client().getTable(DATABASE, TARGET_UNPARTITIONED_MANAGED_TABLE);
        String expectedReplicaSchemaUrl = replicaWarehouseUri.toURI().toString() + "ct_database/";
        String transformedAvroUrl = replicaHiveTable.getParameters().get("avro.schema.url");

        assertThat(transformedAvroUrl, startsWith(expectedReplicaSchemaUrl));
        assertThat(replicaHiveTable.getParameters().get("table.transformed"), is("true"));
      }
    });

    runner.run(config.getAbsolutePath());
  }

  @Test
  public void noTablesReplicated() throws Exception {
    exit.expectSystemExitWithStatus(-1);
    File config = dataFolder.getFile("full-replication-failure.yml");
    CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build()
        .run(config.getAbsolutePath());
  }

  @Test
  public void partitionTableReplicateIgnoringMissingData() throws Exception {
    helper.createPartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_PARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_PARTITIONED_TABLE));

    // Note this is using the DistCpCopier (Not s3). We can't force S3DistcpCopier in the integration tests currently
    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("partitioned-single-table-ignore-missing-folders.yml");
    Partition partition = sourceCatalog
        .client()
        .getPartition(DATABASE, SOURCE_PARTITIONED_TABLE, Arrays.asList("Europe", "UK"));
    // Delete source partition
    File partitionFolder = new File(new Path(partition.getSd().getLocation()).toUri());
    FileUtils.deleteDirectory(partitionFolder);

    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        // The partition is replicated despite missing source folder.
        Partition partition = replicaCatalog
            .client()
            .getPartition(DATABASE, SOURCE_PARTITIONED_TABLE, Arrays.asList("Europe", "UK"));
        assertNotNull(partition);
        File partitionFolder = new File(new Path(partition.getSd().getLocation()).toUri());
        // Hive creates the empty partition folder for us.
        assertTrue(partitionFolder.exists());
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void partitionedView() throws Exception {
    helper.createPartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_PARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_PARTITIONED_TABLE));

    helper.createPartitionedView();
    LOG.info(">>>> VIEW {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_PARTITIONED_VIEW));

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("partitioned-single-view-mirror.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        Table hiveView = replicaCatalog.client().getTable(DATABASE, TARGET_PARTITIONED_VIEW);
        assertThat(hiveView.getDbName(), is(DATABASE));
        assertThat(hiveView.getTableName(), is(TARGET_PARTITIONED_VIEW));
        assertThat(isView(hiveView), is(true));
        assertThat(hiveView.getSd().getCols(), is(DATA_COLUMNS));
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void partitionedViewFailure() throws Exception {
    helper.createPartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_PARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_PARTITIONED_TABLE));

    helper.createPartitionedView();
    LOG.info(">>>> VIEW {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_PARTITIONED_VIEW));

    exit.expectSystemExitWithStatus(-1);
    File config = dataFolder.getFile("partitioned-single-view-mirror-failure.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void unpartitionedView() throws Exception {
    helper.createUnpartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_UNPARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_UNPARTITIONED_TABLE));

    helper.createUnpartitionedView();
    LOG.info(">>>> VIEW {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_UNPARTITIONED_VIEW));

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("unpartitioned-single-view-mirror.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        Table hiveView = replicaCatalog.client().getTable(DATABASE, TARGET_UNPARTITIONED_VIEW);
        assertThat(hiveView.getDbName(), is(DATABASE));
        assertThat(hiveView.getTableName(), is(TARGET_UNPARTITIONED_VIEW));
        assertThat(isView(hiveView), is(true));
        assertThat(hiveView.getSd().getCols(), is(DATA_COLUMNS));
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void unpartitionedViewWithMappings() throws Exception {
    helper.createManagedUnpartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE));
    LOG.info(">>>> Table {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_UNPARTITIONED_TABLE));

    helper.createUnpartitionedView();
    LOG.info(">>>> VIEW {} ", sourceCatalog.client().getTable(DATABASE, SOURCE_UNPARTITIONED_VIEW));

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("unpartitioned-single-view-mirror-with-table-mappings.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        Table hiveView = replicaCatalog.client().getTable(DATABASE, TARGET_UNPARTITIONED_VIEW);
        assertThat(hiveView.getDbName(), is(DATABASE));
        assertThat(hiveView.getTableName(), is(TARGET_UNPARTITIONED_VIEW));
        assertThat(isView(hiveView), is(true));
        assertThat(hiveView.getSd().getCols(), is(DATA_COLUMNS));
        assertThat(hiveView.getViewOriginalText(),
            is(String.format("SELECT * FROM %s.%s", DATABASE, TARGET_UNPARTITIONED_MANAGED_TABLE)));
        assertThat(hiveView.getViewExpandedText(),
            is(String
                .format("SELECT `%s`.`id`, `%s`.`name`, `%s`.`city` FROM `%s`.`%s`", TARGET_UNPARTITIONED_MANAGED_TABLE,
                    TARGET_UNPARTITIONED_MANAGED_TABLE, TARGET_UNPARTITIONED_MANAGED_TABLE, DATABASE,
                    TARGET_UNPARTITIONED_MANAGED_TABLE)));
      }
    });
    runner.run(config.getAbsolutePath());
  }

}
