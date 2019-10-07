/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.hotels.bdp.circustrain.avro.transformation;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

import static junit.framework.TestCase.assertTrue;

import java.io.File;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AvroSerDeTransformationTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final AvroSerDeTransformation avroSerDeTransformation = new AvroSerDeTransformation(new HiveConf(),
      new HiveConf());

  private Table table = new Table();
  private Partition partition = new Partition();

  @Test
  public void applyTable() throws Exception {
    File srcSchemaFile = temporaryFolder.newFile();
    File dstFolder = temporaryFolder.newFolder();

    table.putToParameters("avro.schema.url", srcSchemaFile.toString());

    table = avroSerDeTransformation.apply(table, dstFolder.toString(), "1");

    String replicaBasePath = table.getParameters().get("avro.schema.url");
    replicaBasePath = replicaBasePath.substring(0, replicaBasePath.lastIndexOf("/"));

    assertThat(replicaBasePath, startsWith(dstFolder.toString()));
    assertTrue(new File(replicaBasePath).exists());
  }

  @Test
  public void applyPartition() throws Exception {
    File srcSchemaFile = temporaryFolder.newFile();
    File dstFolder = temporaryFolder.newFolder();

    partition.putToParameters("avro.schema.url", srcSchemaFile.toString());

    partition = avroSerDeTransformation.apply(partition, dstFolder.toString(), "1");

    String replicaBasePath = partition.getParameters().get("avro.schema.url");
    replicaBasePath = replicaBasePath.substring(0, replicaBasePath.lastIndexOf("/"));

    assertThat(replicaBasePath, startsWith(dstFolder.toString()));
    assertTrue(new File(replicaBasePath).exists());
  }

  @Test
  public void getAvroSchemaFileName() throws Exception {
    String dummyUri = "testing/avro.avsc";
    assertThat(avroSerDeTransformation.getAvroSchemaFileName(dummyUri), is("avro.avsc"));
  }

  @Test
  public void addTrailingSlash() throws Exception {
    String dummyUri = "testing/avro";
    dummyUri = avroSerDeTransformation.addTrailingSlash(dummyUri);
    assertThat(dummyUri, is("testing/avro/"));
  }

  @Test
  public void addTrailingSlashOnlyAddsOne() throws Exception {
    String dummyUri = "testing/avro/";
    dummyUri = avroSerDeTransformation.addTrailingSlash(dummyUri);
    assertThat(dummyUri, is("testing/avro/"));
  }

  @Test
  public void nullAvroDestinationFailsInTableApply() throws Exception {
    table.putToParameters("avro.schema.url", "/test/");
    Table result = avroSerDeTransformation.apply(table, null, "1");
    assertThat(result, is(table));
  }

  @Test
  public void nullAvroDestinationFailsInPartitionApply() throws Exception {
    partition.putToParameters("avro.schema.url", "/test/");
    Partition result = avroSerDeTransformation.apply(partition, null, "1");
    assertThat(result, is(partition));
  }
}
