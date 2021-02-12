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
package com.hotels.bdp.circustrain.tool.filter;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORECONNECTURLKEY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREPWD;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME;
import static org.apache.hadoop.mapreduce.MRConfig.FRAMEWORK_NAME;
import static org.apache.hadoop.mapreduce.MRConfig.LOCAL_FRAMEWORK_NAME;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;

public class FilterToolIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(FilterToolIntegrationTest.class);
  private static final String TARGET_CLASSES_PATH = "target/test-classes";
  private static final String TEST_HIVE_SITE_XML = "test-hive-site.xml";
  private static final short PARTITION_LIMIT = (short) 100;
  private static final String PART_00000 = "part-00000";
  private static final String DATABASE = "ct_database";
  private static final String TABLE = "ct_table_1";

  public @Rule TemporaryFolder temporaryFolder = new TemporaryFolder();
  public @Rule ThriftHiveMetaStoreJUnitRule sourceCatalog = new ThriftHiveMetaStoreJUnitRule(DATABASE);
  private File sourceTempUri;
  private File sourceWarehouseUri;
  private File sourceTableUri;
  private String testHiveSiteXml;
  private File ymlFile;
  private PrintStream stdout;
  private PrintStream out;
  private ByteArrayOutputStream capture;

  @Before
  public void init() throws Exception {
    String tableLocation = DATABASE + "/" + TABLE;
    sourceTempUri = temporaryFolder.newFolder("source-temp");
    sourceWarehouseUri = temporaryFolder.newFolder("source-warehouse");
    sourceTableUri = new File(sourceWarehouseUri, tableLocation);
    ymlFile = temporaryFolder.newFile("filter.yml");

    createTable(sourceTableUri);

    HiveMetaStoreClient client = sourceCatalog.client();
    LOG.info(">>>> Table = {}", client.getTable(DATABASE, TABLE));

    // Create hive config
    testHiveSiteXml = getClass().getSimpleName() + "_" + TEST_HIVE_SITE_XML;
    File hiveSite = new File(TARGET_CLASSES_PATH, testHiveSiteXml);
    hiveSite.deleteOnExit();
    try (Writer hiveSiteWriter = new FileWriter(hiveSite)) {
      createHiveSiteXml(hiveSiteWriter);
    }
    stdout = System.out;
    capture = new ByteArrayOutputStream();
    out = new PrintStream(capture);
    System.setOut(out);
  }

  @After
  public void resetStdout() {
    System.setOut(stdout);
    out.close();
  }

  @Test
  public void typical() throws Exception {
    writeYmlFile("local_date < '#{#nowUtc().toString(\"yyyy-MM-dd\") }'");
    Map<String, String> props = getArgs();
    FilterTool.main(getArgsArray(props));
    out.flush();
    String outputToUser = new String(capture.toByteArray());
    assertThat(outputToUser,
        containsString("Partition expression:  local_date < '#{#nowUtc().toString(\"yyyy-MM-dd\") }'"));
    assertThat(outputToUser, containsString(
        "Partition filter:      local_date < '" + DateTime.now(DateTimeZone.UTC).toString("yyyy-MM-dd") + "'"));
    assertThat(outputToUser, containsString("Partition(s) fetched:  [[2000-01-01, 0], [2000-01-02, 0]]"));
  }

  @Test
  public void yamlComment() throws Exception {
    writeYmlFile("local_date < '#{ #nowUtc().toString(\"yyyy-MM-dd\") }'");
    Map<String, String> props = getArgs();
    FilterTool.main(getArgsArray(props));
    out.flush();
    String outputToUser = new String(capture.toByteArray());
    assertThat(outputToUser, containsString("Partition expression:  local_date < '#{"));
    assertThat(outputToUser, containsString("WARNING: There was a problem parsing your expression."));
  }

  private void createTable(File sourceTableUri) throws Exception {
    File partitionEurope = new File(sourceTableUri, "local_date=2000-01-01");
    File partitionUk = new File(partitionEurope, "local_hour=0");
    File dataFileUk = new File(partitionUk, PART_00000);
    FileUtils.writeStringToFile(dataFileUk, "1\tadam\tlondon\n2\tsusan\tglasgow\n");

    File partitionAsia = new File(sourceTableUri, "local_date=2000-01-02");
    File partitionChina = new File(partitionAsia, "local_hour=0");
    File dataFileChina = new File(partitionChina, PART_00000);
    String data = "1\tchun\tbeijing\n2\tshanghai\tmilan\n";
    FileUtils.writeStringToFile(dataFileChina, data);

    HiveMetaStoreClient sourceClient = sourceCatalog.client();

    Table source = new Table();
    source.setDbName(DATABASE);
    source.setTableName(TABLE);
    source.setTableType(TableType.EXTERNAL_TABLE.name());
    source.setParameters(new HashMap<String, String>());

    List<FieldSchema> partitionColumns = Arrays.asList(new FieldSchema("local_date", "string", ""),
        new FieldSchema("local_hour", "string", ""));
    source.setPartitionKeys(partitionColumns);

    List<FieldSchema> dataColumns = Arrays.asList(new FieldSchema("id", "bigint", ""),
        new FieldSchema("name", "string", ""), new FieldSchema("city", "tinyint", ""));

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(dataColumns);
    sd.setLocation(sourceTableUri.toURI().toString());
    sd.setParameters(new HashMap<String, String>());
    sd.setSerdeInfo(new SerDeInfo());

    source.setSd(sd);

    sourceClient.createTable(source);
    LOG.info(">>>> Partitions added: {}",
        +sourceClient.add_partitions(Arrays.asList(newPartition(sd, Arrays.asList("2000-01-01", "0"), partitionUk),
            newPartition(sd, Arrays.asList("2000-01-02", "0"), partitionChina))));
  }

  private Partition newPartition(StorageDescriptor tableStorageDescriptor, List<String> values, File location) {
    Partition partition = new Partition();
    partition.setDbName(DATABASE);
    partition.setTableName(TABLE);
    partition.setValues(values);
    partition.setSd(new StorageDescriptor(tableStorageDescriptor));
    partition.getSd().setLocation(location.toURI().toString());
    return partition;
  }

  private void createHiveSiteXml(Writer hiveSiteWriter) {
    try (XmlWriter xmlWriter = new XmlWriter(hiveSiteWriter, "UTF-8")) {
      xmlWriter.startElement("configuration");
      writeProperty(xmlWriter, FRAMEWORK_NAME, LOCAL_FRAMEWORK_NAME);
      writeProperty(xmlWriter, METASTOREURIS, sourceCatalog.getThriftConnectionUri());
      writeProperty(xmlWriter, METASTORECONNECTURLKEY, sourceCatalog.connectionURL());
      writeProperty(xmlWriter, METASTORE_CONNECTION_DRIVER, sourceCatalog.driverClassName());
      writeProperty(xmlWriter, METASTORE_CONNECTION_USER_NAME, sourceCatalog.conf()
        .getVar(METASTORE_CONNECTION_USER_NAME));
      writeProperty(xmlWriter, METASTOREPWD, sourceCatalog.conf().getVar(METASTOREPWD));
      xmlWriter.endElement();
    }
  }

  private static void writeProperty(XmlWriter xmlWriter, ConfVars confVar, String value) {
    writeProperty(xmlWriter, confVar.varname, value);
  }

  private static void writeProperty(XmlWriter xmlWriter, String name, String value) {
    xmlWriter.startElement("property");
    xmlWriter.dataElement("name", name);
    xmlWriter.dataElement("value", value);
    xmlWriter.endElement();
  }

  protected Map<String, String> getArgs() {
    Map<String, String> props = new HashMap<>();
    props.put("config", ymlFile.getAbsolutePath());
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

  private void writeYmlFile(String partitionFilter) throws IOException {
    FileUtils.writeStringToFile(ymlFile, generateYml(partitionFilter));
  }

  private String generateYml(String partitionFilter) {
    StringBuilder yml = new StringBuilder();
    yml.append("replica-catalog:\n");
    yml.append("  name: replicaCatalogDatabaseName\n");
    yml.append("  hive-metastore-uris: thrift://replicaCatalogThriftConnectionUri\n");
    yml.append("source-catalog:\n");
    yml.append("  name: source-");
    yml.append(sourceCatalog.databaseName());
    yml.append('\n');
    yml.append("  temporary-path: ");
    yml.append(sourceTempUri.toURI().toString());
    yml.append('\n');
    yml.append("  disable-snapshots: true\n");
    yml.append("  site-xml: ");
    yml.append(testHiveSiteXml);
    yml.append('\n');
    yml.append("copier-options:\n");
    yml.append("  file-attribute: replication, blocksize, user, group, permission, checksumtype\n");
    yml.append("  preserve-raw-xattrs: false\n");
    yml.append("table-replications:\n");
    yml.append("  -\n");
    yml.append("    source-table:\n");
    yml.append("      database-name: ");
    yml.append(DATABASE);
    yml.append('\n');
    yml.append("      table-name: ");
    yml.append(TABLE);
    yml.append('\n');
    yml.append("      partition-filter: ");
    yml.append(partitionFilter);
    yml.append('\n');
    yml.append("      partition-limit: ");
    yml.append(String.valueOf(PARTITION_LIMIT));
    yml.append('\n');
    yml.append("    replica-table:\n");
    yml.append("      table-location: replicaTableUri\n");
    return yml.toString();
  }

}
