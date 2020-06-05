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

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.REPLICATION_EVENT;
import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.REPLICATION_MODE;
import static com.hotels.bdp.circustrain.integration.IntegrationTestHelper.DATABASE;
import static com.hotels.bdp.circustrain.integration.IntegrationTestHelper.PART_00000;
import static com.hotels.bdp.circustrain.integration.IntegrationTestHelper.SOURCE_MANAGED_PARTITIONED_TABLE;
import static com.hotels.bdp.circustrain.integration.IntegrationTestHelper.UNPARTITIONED_TABLE;
import static com.hotels.bdp.circustrain.integration.utils.TestUtils.DATA_COLUMNS;
import static com.hotels.bdp.circustrain.integration.utils.TestUtils.newTablePartition;
import static com.hotels.bdp.circustrain.integration.utils.TestUtils.toUri;
import static com.hotels.bdp.circustrain.s3s3copier.aws.AmazonS3URIs.toAmazonS3URI;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.Assertion;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.rules.TemporaryFolder;

import fm.last.commons.test.file.ClassDataFolder;
import fm.last.commons.test.file.DataFolder;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.circustrain.api.conf.Security;
import com.hotels.bdp.circustrain.common.test.base.CircusTrainRunner;
import com.hotels.bdp.circustrain.integration.utils.TestUtils;
import com.hotels.bdp.circustrain.s3s3copier.S3S3CopierOptions;
import com.hotels.bdp.circustrain.s3s3copier.aws.AmazonS3ClientFactory;
import com.hotels.bdp.circustrain.s3s3copier.aws.JceksAmazonS3ClientFactory;
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;

/**
 * Integration test class for checking replication modes. Uses s3-s3 as this is the most commonly used file system for
 * replication.
 */
public class CircusTrainReplicationModeIntegrationTest {

  private static final String S3_ACCESS_KEY = "access";
  private static final String S3_SECRET_KEY = "secret";

  private static final String TARGET_UNPARTITIONED_TABLE = "ct_table_u_copy";
  private static final String TARGET_PARTITIONED_TABLE = "ct_table_p_copy";

  public @Rule ExpectedSystemExit exit = ExpectedSystemExit.none();
  public @Rule TemporaryFolder temporaryFolder = new TemporaryFolder();
  public @Rule DataFolder dataFolder = new ClassDataFolder();
  private final int s3ProxyPort = TestUtils.getAvailablePort();
  public @Rule S3ProxyRule s3Proxy = S3ProxyRule
      .builder()
      .withPort(s3ProxyPort)
      .withCredentials(S3_ACCESS_KEY, S3_SECRET_KEY)
      .ignoreUnknownHeaders()
      .build();
  private final Map<String, String> metastoreProperties = ImmutableMap
      .<String, String>builder()
      .put(ENDPOINT, String.format("http://127.0.0.1:%d", s3ProxyPort))
      .put(ACCESS_KEY, S3_ACCESS_KEY)
      .put(SECRET_KEY, S3_SECRET_KEY)
      .build();
  public @Rule ThriftHiveMetaStoreJUnitRule sourceCatalog = new ThriftHiveMetaStoreJUnitRule(DATABASE,
      metastoreProperties);
  public @Rule ThriftHiveMetaStoreJUnitRule replicaCatalog = new ThriftHiveMetaStoreJUnitRule(DATABASE,
      metastoreProperties);

  private File sourceWarehouseUri;
  private File replicaWarehouseUri;
  private File housekeepingDbLocation;

  private IntegrationTestHelper helper;

  private String jceksLocation;
  private AmazonS3ClientFactory s3ClientFactory;
  private AmazonS3 s3Client;

  @Before
  public void init() throws Exception {
    sourceWarehouseUri = temporaryFolder.newFolder("source-warehouse");
    replicaWarehouseUri = temporaryFolder.newFolder("replica-warehouse");
    temporaryFolder.newFolder("db");
    housekeepingDbLocation = new File(new File(temporaryFolder.getRoot(), "db"), "housekeeping");

    helper = new IntegrationTestHelper(sourceCatalog.client());

    jceksLocation = String.format("jceks://file/%s/aws.jceks", dataFolder.getFolder().getAbsolutePath());
    Security security = new Security();
    security.setCredentialProvider(jceksLocation);
    s3ClientFactory = new JceksAmazonS3ClientFactory(security);

    s3Client = newS3Client("s3a://source/");
    s3Client.createBucket("source");
    s3Client.createBucket("replica");
  }

  private AmazonS3 newS3Client(String tableUri) {
    AmazonS3URI base = toAmazonS3URI(URI.create(tableUri));
    S3S3CopierOptions s3s3CopierOptions = new S3S3CopierOptions(ImmutableMap
        .<String, Object>builder()
        .put(S3S3CopierOptions.Keys.S3_ENDPOINT_URI.keyName(), s3Proxy.getUri().toString())
        .build());
    return s3ClientFactory.newInstance(base, s3s3CopierOptions);
  }

  @Test
  public void unpartitionedTableFullOverwrite() throws Exception {
    final URI sourceTableLocation = toUri("s3a://source/", DATABASE, UNPARTITIONED_TABLE);
    TestUtils.createUnpartitionedTable(sourceCatalog.client(), DATABASE, UNPARTITIONED_TABLE, sourceTableLocation);
    // adjusting the sourceTable, mimicking the change we want to update
    Table sourceTable = sourceCatalog.client().getTable(DATABASE, UNPARTITIONED_TABLE);
    sourceTable.putToParameters("paramToUpdate", "updated");
    sourceCatalog.client().alter_table(sourceTable.getDbName(), sourceTable.getTableName(), sourceTable);

    final File dataFile = temporaryFolder.newFile();
    FileUtils.writeStringToFile(dataFile, "1\trob\tbristol\n2\tsam\ttoronto\n");
    String fileKey = String.format("%s/%s/%s", DATABASE, UNPARTITIONED_TABLE, PART_00000);
    s3Client.putObject("source", fileKey, dataFile);

    // creating replicaTable
    final URI replicaLocation = toUri("s3a://replica/", DATABASE, UNPARTITIONED_TABLE);
    TestUtils.createUnpartitionedTable(replicaCatalog.client(), DATABASE, TARGET_UNPARTITIONED_TABLE, replicaLocation);
    Table replicaTable = replicaCatalog.client().getTable(DATABASE, TARGET_UNPARTITIONED_TABLE);
    // setting up parameters and additional columns
    setupReplicaTable(replicaTable, false, replicaLocation);
    replicaCatalog.client().alter_table(replicaTable.getDbName(), replicaTable.getTableName(), replicaTable);

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("unpartitioned-single-table-full-overwrite-replication.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .copierOption(S3S3CopierOptions.Keys.S3_ENDPOINT_URI.keyName(), s3Proxy.getUri().toString())
        .sourceConfigurationProperty(ENDPOINT, s3Proxy.getUri().toString())
        .replicaConfigurationProperty(ENDPOINT, s3Proxy.getUri().toString())
        .replicaConfigurationProperty(ACCESS_KEY, s3Proxy.getAccessKey())
        .replicaConfigurationProperty(SECRET_KEY, s3Proxy.getSecretKey())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        Table hiveTable = replicaCatalog.client().getTable(DATABASE, TARGET_UNPARTITIONED_TABLE);
        String eventId = hiveTable.getParameters().get(REPLICATION_EVENT.parameterName());
        URI replicaLocation = toUri("s3a://replica/", DATABASE, TARGET_UNPARTITIONED_TABLE + "/" + eventId);
        assertThat(hiveTable.getSd().getLocation(), is(replicaLocation.toString()));
        assertThat(eventId, startsWith("ctt-"));
        assertThat(hiveTable.getParameters().get(REPLICATION_MODE.parameterName()), is("FULL_OVERWRITE"));
        assertThat(hiveTable.getParameters().get("paramToUpdate"), is("updated"));
        assertThat(hiveTable.getSd().getCols(), is(DATA_COLUMNS));

        // Assert files copied from source
        List<S3ObjectSummary> replicaFiles = TestUtils.listObjects(s3Client, "replica");
        assertThat(replicaFiles.size(), is(1));
        assertThat(replicaFiles.get(0).getSize(), is(dataFile.length()));
        String fileKey = String.format("%s/%s/%s/%s", DATABASE, TARGET_UNPARTITIONED_TABLE, eventId, PART_00000);
        assertThat(replicaFiles.get(0).getKey(), is(fileKey));
      }
    });
    runner.run(config.getAbsolutePath());
  }

  @Test
  public void partitionedTableFullOverwrite() throws Exception {
    helper.createManagedPartitionedTable(toUri(sourceWarehouseUri, DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE));
    // adjusting the sourceTable, mimicking the change we want to update
    Table sourceTable = sourceCatalog.client().getTable(DATABASE, SOURCE_MANAGED_PARTITIONED_TABLE);
    sourceTable.putToParameters("paramToUpdate", "updated");
    sourceCatalog.client().alter_table(sourceTable.getDbName(), sourceTable.getTableName(), sourceTable);

    final URI replicaLocation = toUri("s3a://replica/", DATABASE, TARGET_PARTITIONED_TABLE);
    TestUtils.createPartitionedTable(replicaCatalog.client(), DATABASE, TARGET_PARTITIONED_TABLE, replicaLocation);
    Table replicaTable = replicaCatalog.client().getTable(DATABASE, TARGET_PARTITIONED_TABLE);

    // setting up parameters, additional columns and partitions
    setupReplicaTable(replicaTable, true, replicaLocation);
    replicaCatalog.client().alter_table(replicaTable.getDbName(), replicaTable.getTableName(), replicaTable);

    exit.expectSystemExitWithStatus(0);
    File config = dataFolder.getFile("partitioned-single-table-full-overwrite-replication.yml");
    CircusTrainRunner runner = CircusTrainRunner
        .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
        .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
            sourceCatalog.driverClassName())
        .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
        .copierOption(S3S3CopierOptions.Keys.S3_ENDPOINT_URI.keyName(), s3Proxy.getUri().toString())
        .sourceConfigurationProperty(ENDPOINT, s3Proxy.getUri().toString())
        .replicaConfigurationProperty(ENDPOINT, s3Proxy.getUri().toString())
        .replicaConfigurationProperty(ACCESS_KEY, s3Proxy.getAccessKey())
        .replicaConfigurationProperty(SECRET_KEY, s3Proxy.getSecretKey())
        .build();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        Table hiveTable = replicaCatalog.client().getTable(DATABASE, TARGET_PARTITIONED_TABLE);
        URI replicaLocation = toUri("s3a://replica/", DATABASE, TARGET_PARTITIONED_TABLE);
        assertThat(hiveTable.getSd().getLocation(), is(replicaLocation.toString()));
        assertThat(hiveTable.getParameters().get(REPLICATION_EVENT.parameterName()), startsWith("ctp-"));
        assertThat(hiveTable.getParameters().get(REPLICATION_MODE.parameterName()), is("FULL_OVERWRITE"));
        assertThat(hiveTable.getParameters().get("paramToUpdate"), is("updated"));
        assertThat(hiveTable.getSd().getCols(), is(DATA_COLUMNS));

        List<Partition> partitions = replicaCatalog
            .client()
            .listPartitions(DATABASE, TARGET_PARTITIONED_TABLE, (short) -1);
        assertThat(partitions.size(), is(2));
        assertThat(partitions.get(0).getValues(), is(Arrays.asList("Asia", "China")));
        assertThat(partitions.get(1).getValues(), is(Arrays.asList("Europe", "UK")));
      }
    });
    runner.run(config.getAbsolutePath());
  }

  private void setupReplicaTable(Table replicaTable, boolean partitioned, URI replicaLocation) throws Exception {
    addColumnsToReplica(replicaTable);
    setupReplicaParameters(replicaTable);
    if (partitioned) {
      addPartitionsToReplica(replicaTable, replicaLocation);
    }
  }

  private void addColumnsToReplica(Table replicaTable) throws Exception {
    List<FieldSchema> columns = replicaTable.getSd().getCols();
    columns.add(new FieldSchema("age", "int", ""));
    columns.add(new FieldSchema("colour", "string", ""));
    replicaTable.getSd().setCols(columns);
  }

  private void setupReplicaParameters(Table replicaTable) {
    replicaTable.putToParameters(REPLICATION_EVENT.parameterName(), "dummyEventID");
    replicaTable.putToParameters("paramToUpdate", "update-me");
  }

  private void addPartitionsToReplica(Table replicaTable, URI replicaLocation) throws Exception {
    URI partitionAmerica = URI.create(replicaLocation + "/dummyEventID/continent=America");
    final URI partitionMexico = URI.create(partitionAmerica + "/country=Mexico");
    replicaCatalog
        .client()
        .add_partitions(
            Arrays.asList(newTablePartition(replicaTable, Arrays.asList("America", "Mexico"), partitionMexico)));
  }

}