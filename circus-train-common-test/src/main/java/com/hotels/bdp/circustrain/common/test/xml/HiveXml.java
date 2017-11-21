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
package com.hotels.bdp.circustrain.common.test.xml;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORECONNECTURLKEY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREPWD;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME;
import static org.apache.hadoop.mapreduce.MRConfig.FRAMEWORK_NAME;
import static org.apache.hadoop.mapreduce.MRConfig.LOCAL_FRAMEWORK_NAME;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

public class HiveXml {

  public static final String TARGET_CLASSES_PATH = "target/test-classes";

  private static final String TEST_HIVE_SITE_XML = "test-hive-site.xml";

  private final String hiveSite;
  private final String thriftConnectionUri;
  private final String connectionURL;
  private final String driverClassName;

  public HiveXml(String thriftConnectionUri, String connectionURL, String driverClassName) {
    this.thriftConnectionUri = thriftConnectionUri;
    this.connectionURL = connectionURL;
    this.driverClassName = driverClassName;
    hiveSite = UUID.randomUUID() + "_" + TEST_HIVE_SITE_XML;
  }

  public String getFileName() {
    return hiveSite;
  }

  public File create() {
    File hiveSiteFile = new File(TARGET_CLASSES_PATH, hiveSite);
    hiveSiteFile.deleteOnExit();
    try (Writer hiveSiteWriter = new OutputStreamWriter(new FileOutputStream(hiveSiteFile), StandardCharsets.UTF_8)) {
      createHiveSiteXml(hiveSiteWriter);
    } catch (IOException e) {
      throw new RuntimeException("Unable to create " + hiveSite, e);
    }
    return hiveSiteFile;
  }

  private void createHiveSiteXml(Writer hiveSiteWriter) {
    try (XmlWriter xmlWriter = new XmlWriter(hiveSiteWriter, StandardCharsets.UTF_8.name())) {
      xmlWriter.startElement("configuration");
      writeProperty(xmlWriter, FRAMEWORK_NAME, LOCAL_FRAMEWORK_NAME);
      writeProperty(xmlWriter, METASTOREURIS, thriftConnectionUri);
      writeProperty(xmlWriter, METASTORECONNECTURLKEY, connectionURL);
      writeProperty(xmlWriter, METASTORE_CONNECTION_DRIVER, driverClassName);
      // See com.hotels.beeju.BeejuJUnitRule.METASTORE_DB_USER
      writeProperty(xmlWriter, METASTORE_CONNECTION_USER_NAME, "db_user");
      // See com.hotels.beeju.BeejuJUnitRule.METASTORE_DB_PASSWORD
      writeProperty(xmlWriter, METASTOREPWD, "db_password");
      xmlWriter.endElement();
    }
  }

  private static void writeProperty(XmlWriter xmlWriter, ConfVars confVar, String value) {
    writeProperty(xmlWriter, confVar.varname, value);
  }

  private static void writeProperty(XmlWriter xmlWriter, String name, String value) {
    if (value != null) {
      xmlWriter.startElement("property");
      xmlWriter.dataElement("name", name);
      xmlWriter.dataElement("value", value);
      xmlWriter.endElement();
    }
  }

}
