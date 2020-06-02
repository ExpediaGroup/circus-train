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
package com.hotels.bdp.circustrain;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static com.google.common.base.Charsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.Assertion;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.rules.TemporaryFolder;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.conf.DataManipulationClient;
import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.copier.CopierFactory;
import com.hotels.bdp.circustrain.api.metrics.Metrics;
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;
import com.hotels.test.extension.TestLocomotiveListener;

public class CircusTrainTest {

  private static final String DATABASE = "test_database";
  private static final String TABLE = "test_table";

  public @Rule ExpectedSystemExit exit = ExpectedSystemExit.none();
  public @Rule ThriftHiveMetaStoreJUnitRule hive = new ThriftHiveMetaStoreJUnitRule(DATABASE);
  public @Rule TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void before() throws TException, IOException {
    Table table = new Table();
    table.setDbName(DATABASE);
    table.setTableName("source_" + TABLE);
    table.setTableType(TableType.EXTERNAL_TABLE.name());
    table.putToParameters("EXTERNAL", "TRUE");

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(Arrays.asList(new FieldSchema("col1", "string", null)));
    sd.setSerdeInfo(new SerDeInfo());
    table.setSd(sd);

    hive.client().createTable(table);
  }

  @Test
  public void ymlFileDoesNotExist() throws Exception {
    exit.expectSystemExitWithStatus(-1);
    File ymlFile = new File(temp.getRoot(), "test-application.yml");
    CircusTrain.main(new String[] { "--config=" + ymlFile.getAbsolutePath() });
  }

  @Test
  public void singleYmlFile() throws Exception {
    exit.expectSystemExitWithStatus(0);
    File ymlFile = temp.newFile("test-application.yml");
    List<String> lines = ImmutableList
        .<String>builder()
        .add("extension-packages: " + TestCopierFactory.class.getPackage().getName())
        .add("source-catalog:")
        .add("  name: source")
        .add("  configuration-properties:")
        .add("    " + ConfVars.METASTOREURIS.varname + ": " + hive.getThriftConnectionUri())
        .add("replica-catalog:")
        .add("  name: replica")
        .add("  hive-metastore-uris: " + hive.getThriftConnectionUri())
        .add("table-replications:")
        .add("  -")
        .add("    source-table:")
        .add("      database-name: " + DATABASE)
        .add("      table-name: source_" + TABLE)
        .add("    replica-table:")
        .add("      table-name: replica_" + TABLE)
        .add("      table-location: " + temp.newFolder("replica"))
        .build();
    Files.asCharSink(ymlFile, UTF_8).writeLines(lines);

    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        assertTrue(hive.client().tableExists(DATABASE, "replica_" + TABLE));
      }
    });

    CircusTrain.main(new String[] { "--config=" + ymlFile.getAbsolutePath() });
  }

  @Test
  public void singleYmlFileWithMultipleExtensions() throws Exception {
    TestLocomotiveListener.testBean = null;
    exit.expectSystemExitWithStatus(0);
    File ymlFile = temp.newFile("test-application.yml");
    List<String> lines = ImmutableList
        .<String>builder()
        .add("source-catalog:")
        .add("  name: source")
        .add("  configuration-properties:")
        .add("    " + ConfVars.METASTOREURIS.varname + ": " + hive.getThriftConnectionUri())
        .add("replica-catalog:")
        .add("  name: replica")
        .add("  hive-metastore-uris: " + hive.getThriftConnectionUri())
        .add("table-replications:")
        .add("  -")
        .add("    source-table:")
        .add("      database-name: " + DATABASE)
        .add("      table-name: source_" + TABLE)
        .add("    replica-table:")
        .add("      table-name: replica_" + TABLE)
        .add("      table-location: " + temp.newFolder("replica"))
        .add("extension-packages: com.hotels.test.extension, " + TestCopierFactory.class.getPackage().getName())
        .add("testExtensionConfig: foo")
        .build();
    Files.asCharSink(ymlFile, UTF_8).writeLines(lines);

    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        assertThat(TestLocomotiveListener.testBean.getValue(), is("foo"));
      }
    });
    CircusTrain.main(new String[] { "--config=" + ymlFile.getAbsolutePath() });
  }

  @Test
  public void twoYmlFiles() throws Exception {
    exit.expectSystemExitWithStatus(0);
    File ymlFile1 = temp.newFile("test-application1.yml");
    File ymlFile2 = temp.newFile("test-application2.yml");

    List<String> lines = ImmutableList
        .<String>builder()
        .add("extension-packages: " + TestCopierFactory.class.getPackage().getName())
        .add("source-catalog:")
        .add("  name: source")
        .add("  configuration-properties:")
        .add("    " + ConfVars.METASTOREURIS.varname + ": " + hive.getThriftConnectionUri())
        .add("replica-catalog:")
        .add("  name: replica")
        .add("  hive-metastore-uris: " + hive.getThriftConnectionUri())
        .build();
    Files.asCharSink(ymlFile1, UTF_8).writeLines(lines);

    lines = ImmutableList
        .<String>builder()
        .add("table-replications:")
        .add("  -")
        .add("    source-table:")
        .add("      database-name: " + DATABASE)
        .add("      table-name: source_" + TABLE)
        .add("    replica-table:")
        .add("      table-name: replica_" + TABLE)
        .add("      table-location: " + temp.newFolder("replica"))
        .build();
    Files.asCharSink(ymlFile2, UTF_8).writeLines(lines);

    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        assertTrue(hive.client().tableExists(DATABASE, "replica_" + TABLE));
      }
    });

    CircusTrain.main(new String[] { "--config=" + ymlFile1.getAbsolutePath() + "," + ymlFile2.getAbsolutePath() });
  }

  @Test
  public void emptyYmlFile() throws Exception {
    exit.expectSystemExitWithStatus(-1);
    File ymlFile = temp.newFile("test-application.yml");
    CircusTrain.main(new String[] { "--config=" + ymlFile.getAbsolutePath() });
  }

  @Component
  static class TestCopierFactory implements CopierFactory {

    @Override
    public boolean supportsSchemes(String sourceScheme, String replicaScheme) {
      return true;
    }

    @Override
    public Copier newInstance(
        String eventId,
        Path sourceBaseLocation,
        List<Path> sourceSubLocations,
        Path replicaLocation,
        Map<String, Object> copierOptions) {
      return new TestCopier();
    }

    @Override
    public Copier newInstance(
        String eventId,
        Path sourceBaseLocation,
        Path replicaLocation,
        Map<String, Object> copierOptions) {
      return newInstance(eventId, sourceBaseLocation, Collections.<Path>emptyList(), replicaLocation, copierOptions);
    }

  }

  static class TestCopier implements Copier {

    @Override
    public Metrics copy() throws CircusTrainException {
      return Metrics.NULL_VALUE;
    }

    @Override
    public DataManipulationClient getClient() {
      return null;
    }

  }

}
