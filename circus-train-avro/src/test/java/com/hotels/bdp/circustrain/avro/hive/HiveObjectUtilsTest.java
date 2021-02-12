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
package com.hotels.bdp.circustrain.avro.hive;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import static junit.framework.TestCase.assertTrue;

import static com.hotels.bdp.circustrain.avro.TestUtils.newPartition;
import static com.hotels.bdp.circustrain.avro.TestUtils.newTable;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

public class HiveObjectUtilsTest {
  private static final String AVRO_SCHEMA_URL_PARAMETER = "avro.schema.url";

  @Test
  public void getParameterFromTablesTableProperties() {
    Table table = newTable();
    table.getParameters().put(AVRO_SCHEMA_URL_PARAMETER, "test");
    assertThat(HiveObjectUtils.getParameter(table, AVRO_SCHEMA_URL_PARAMETER), is("test"));
  }

  @Test
  public void getParameterFromTablesSerDeProperties() {
    Table table = newTable();
    table.getSd().getSerdeInfo().getParameters().put(AVRO_SCHEMA_URL_PARAMETER, "test");
    assertThat(HiveObjectUtils.getParameter(table, AVRO_SCHEMA_URL_PARAMETER), is("test"));
  }

  @Test
  public void getParameterFromTableWhichIsntSetReturnsNull() {
    Table table = newTable();
    assertThat(HiveObjectUtils.getParameter(table, AVRO_SCHEMA_URL_PARAMETER), is(nullValue()));
  }

  @Test
  public void getParameterFromTablePropertiesWhenSerDePropertiesAreAlsoSetInTable() {
    Table table = newTable();
    table.getParameters().put(AVRO_SCHEMA_URL_PARAMETER, "test");
    table.getSd().getSerdeInfo().getParameters().put(AVRO_SCHEMA_URL_PARAMETER, "foo");
    assertThat(HiveObjectUtils.getParameter(table, AVRO_SCHEMA_URL_PARAMETER), is("test"));
  }

  @Test
  public void updateUrlInTable() {
    Table table = newTable();
    table.getParameters().put(AVRO_SCHEMA_URL_PARAMETER, "test");
    table.getSd().getSerdeInfo().getParameters().put(AVRO_SCHEMA_URL_PARAMETER, "test");
    HiveObjectUtils.updateSerDeUrl(table, AVRO_SCHEMA_URL_PARAMETER, "updatedUrl");

    assertThat(table.getParameters().get(AVRO_SCHEMA_URL_PARAMETER), is("updatedUrl"));
    assertThat(table.getSd().getSerdeInfo().getParameters().get(AVRO_SCHEMA_URL_PARAMETER), is(nullValue()));
  }

  @Test
  public void getParameterFromPartitionsTableProperties() {
    Partition partition = newPartition();
    partition.getParameters().put(AVRO_SCHEMA_URL_PARAMETER, "test");
    assertThat(HiveObjectUtils.getParameter(partition, AVRO_SCHEMA_URL_PARAMETER), is("test"));
  }

  @Test
  public void getParameterFromPartitionsSerDeProperties() {
    Partition partition = newPartition();
    partition.getSd().getSerdeInfo().getParameters().put(AVRO_SCHEMA_URL_PARAMETER, "test");
    assertThat(HiveObjectUtils.getParameter(partition, AVRO_SCHEMA_URL_PARAMETER), is("test"));
  }

  @Test
  public void getParameterFromTablePropertiesWhenSerDePropertiesAreAlsoSetInPartition() {
    Partition partition = newPartition();
    partition.getParameters().put(AVRO_SCHEMA_URL_PARAMETER, "test");
    partition.getSd().getSerdeInfo().getParameters().put(AVRO_SCHEMA_URL_PARAMETER, "foo");
    assertThat(HiveObjectUtils.getParameter(partition, AVRO_SCHEMA_URL_PARAMETER), is("test"));
  }

  @Test
  public void getParameterFromPartitionWhichIsntSetReturnsNull() {
    Partition partition = newPartition();
    assertTrue(HiveObjectUtils.getParameter(partition, AVRO_SCHEMA_URL_PARAMETER) == null);
  }

  @Test
  public void updateUrlInPartition() {
    Partition partition = newPartition();
    partition.getParameters().put(AVRO_SCHEMA_URL_PARAMETER, "test");
    partition.getSd().getSerdeInfo().getParameters().put(AVRO_SCHEMA_URL_PARAMETER, "test");
    HiveObjectUtils.updateSerDeUrl(partition, AVRO_SCHEMA_URL_PARAMETER, "updatedUrl");

    assertThat(partition.getParameters().get(AVRO_SCHEMA_URL_PARAMETER), is("updatedUrl"));
    assertThat(partition.getSd().getSerdeInfo().getParameters().get(AVRO_SCHEMA_URL_PARAMETER), is(nullValue()));
  }

}
