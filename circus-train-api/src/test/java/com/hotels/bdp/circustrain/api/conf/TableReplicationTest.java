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
package com.hotels.bdp.circustrain.api.conf;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import org.junit.Before;
import org.junit.Test;

public class TableReplicationTest {

  private SourceTable sourceTable;
  private ReplicaTable replicaTable;
  private TableReplication tableReplication;
  private Validator validator;

  @Before
  public void before() {
    ValidatorFactory config = Validation.buildDefaultValidatorFactory();
    validator = config.getValidator();
  }

  @Before
  public void buildConfig() {
    sourceTable = new SourceTable();
    sourceTable.setDatabaseName("source-database");
    sourceTable.setTableName("source-table");
    replicaTable = new ReplicaTable();
    replicaTable.setDatabaseName("replica-database");
    replicaTable.setTableName("replica-table");
    tableReplication = new TableReplication();
    tableReplication.setSourceTable(sourceTable);
    tableReplication.setReplicaTable(replicaTable);
  }

  @Test
  public void getQualifiedReplicaName() {
    assertThat(tableReplication.getQualifiedReplicaName(), is("replica-database.replica-table"));
  }

  @Test
  public void getQualifiedReplicaNameUseSourceDatabase() {
    replicaTable.setDatabaseName(null);
    assertThat(tableReplication.getQualifiedReplicaName(), is("source-database.replica-table"));
  }

  @Test
  public void getQualifiedReplicaNameUseSourceTable() {
    replicaTable.setTableName(null);
    assertThat(tableReplication.getQualifiedReplicaName(), is("replica-database.source-table"));
  }

  @Test
  public void validTableLocation() throws Exception {
    replicaTable.setTableLocation("tableLocation");

    Set<ConstraintViolation<TableReplication>> violations = validator.validate(tableReplication);

    assertThat(violations.size(), is(0));
  }

  @Test
  public void nullTableLocation() throws Exception {
    replicaTable.setTableLocation(null);

    Set<ConstraintViolation<TableReplication>> violations = validator.validate(tableReplication);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void mergeNullOptions() {
    Map<String, Object> mergedCopierOptions = tableReplication.getMergedCopierOptions(null);
    assertThat(mergedCopierOptions, is(not(nullValue())));
    assertThat(mergedCopierOptions.isEmpty(), is(true));
  }

  @Test
  public void mergeOptions() {
    Map<String, Object> globalOptions = new HashMap<>();
    globalOptions.put("one", Integer.valueOf(1));
    globalOptions.put("two", Integer.valueOf(2));
    Map<String, Object> overrideOptions = new HashMap<>();
    overrideOptions.put("two", "two");
    overrideOptions.put("three", "three");
    tableReplication.setCopierOptions(overrideOptions);
    Map<String, Object> mergedCopierOptions = tableReplication.getMergedCopierOptions(globalOptions);
    assertThat((Integer) mergedCopierOptions.get("one"), is(Integer.valueOf(1)));
    assertThat((String) mergedCopierOptions.get("two"), is("two"));
    assertThat((String) mergedCopierOptions.get("three"), is("three"));
  }

}
