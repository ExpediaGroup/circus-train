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
package com.hotels.bdp.circustrain.core.transformation;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.conf.TransformOptions;
import com.hotels.bdp.circustrain.api.event.EventTableReplication;
import com.hotels.bdp.circustrain.core.conf.CircusTrainTransformOptions;

@RunWith(MockitoJUnitRunner.class)
public class TableParametersTransformationTest {

  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final String OVERRIDE_KEY = "override_key";
  private static final String OVERRIDE_VALUE = "override_value";
  private static final String SECOND_OVERRIDE_KEY = "second_override_key";
  private static final String SECOND_OVERRIDE_VALUE = "second_override_value";
  private static final String EVENT_ID = "event_id";
  private Table table = new Table();
  private TableParametersTransformation transformation;

  @Before
  public void init() {
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put(KEY, VALUE);
    TransformOptions transformOptions = new TransformOptions();
    transformOptions.setTableProperties(tableProperties);
    transformation = new TableParametersTransformation(transformOptions);
  }

  @Test
  public void typical() {
    Table transformedTable = transformation.transform(table);
    Map<String, String> tableParameters = transformedTable.getParameters();
    assertThat(tableParameters.size(), is(1));
    assertThat(tableParameters.get(KEY), is(VALUE));
  }

  @Test
  public void typicalImmutableTableParameters() {
    table.setParameters(Collections.EMPTY_MAP);
    Table transformedTable = transformation.transform(table);
    Map<String, String> tableParameters = transformedTable.getParameters();
    assertThat(tableParameters.size(), is(1));
    assertThat(tableParameters.get(KEY), is(VALUE));
  }

  @Test
  public void typicalWithTableParameters() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("old_key", "old_value");
    table.setParameters(parameters);
    Table transformedTable = transformation.transform(table);
    Map<String, String> tableParameters = transformedTable.getParameters();
    assertThat(tableParameters.size(), is(2));
    assertThat(tableParameters.get("old_key"), is("old_value"));
    assertThat(tableParameters.get(KEY), is(VALUE));
  }

  @Test
  public void transformationParametersOverwriteTableParameters() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put(KEY, "old_value");
    table.setParameters(parameters);
    Table transformedTable = transformation.transform(table);
    Map<String, String> tableParameters = transformedTable.getParameters();
    assertThat(tableParameters.size(), is(1));
    assertThat(tableParameters.get(KEY), is(VALUE));
  }

  @Test
  public void typicalOverride() {
    transformation.tableReplicationStart(createEventTableReplication(OVERRIDE_KEY, OVERRIDE_VALUE), EVENT_ID);
    Table transformedTable = transformation.transform(table);
    Map<String, String> tableParameters = transformedTable.getParameters();
    assertThat(tableParameters.size(), is(1));
    assertThat(tableParameters.get(OVERRIDE_KEY), is(OVERRIDE_VALUE));
  }

  @Test
  public void typicalTwoReplicationsBothOverride() {
    transformation.tableReplicationStart(createEventTableReplication(OVERRIDE_KEY, OVERRIDE_VALUE),EVENT_ID);
    transformation.transform(table);
    assertThat(table.getParameters().size(), is(1));
    assertThat(table.getParameters().get(OVERRIDE_KEY), is(OVERRIDE_VALUE));
    transformation.tableReplicationStart(createEventTableReplication(SECOND_OVERRIDE_KEY, SECOND_OVERRIDE_VALUE),
      EVENT_ID);
    Table transformedTable = transformation.transform(new Table());
    Map<String, String> tableParameters = transformedTable.getParameters();
    assertThat(tableParameters.size(), is(1));
    assertThat(tableParameters.get(SECOND_OVERRIDE_KEY), is(SECOND_OVERRIDE_VALUE));
  }

  @Test
  public void typicalTwoReplicationsFirstOverride() {
    transformation.tableReplicationStart(createEventTableReplication(OVERRIDE_KEY, OVERRIDE_VALUE),EVENT_ID);
    transformation.transform(table);
    assertThat(table.getParameters().size(), is(1));
    assertThat(table.getParameters().get(OVERRIDE_KEY), is(OVERRIDE_VALUE));
    transformation.tableReplicationStart(createEventTableReplication(Collections.EMPTY_MAP), EVENT_ID);
    Table transformedTable = transformation.transform(new Table());
    Map<String, String> tableParameters = transformedTable.getParameters();
    assertThat(tableParameters.size(), is(1));
    assertThat(tableParameters.get(KEY), is(VALUE));
  }

  @Test
  public void typicalTwoReplicationsSecondOverride() {
    transformation.tableReplicationStart(createEventTableReplication(Collections.EMPTY_MAP),EVENT_ID);
    transformation.transform(table);
    assertThat(table.getParameters().size(), is(1));
    assertThat(table.getParameters().get(KEY), is(VALUE));
    transformation.tableReplicationStart(createEventTableReplication(SECOND_OVERRIDE_KEY, SECOND_OVERRIDE_VALUE),
      EVENT_ID);
    Table transformedTable = transformation.transform(new Table());
    Map<String, String> tableParameters = transformedTable.getParameters();
    assertThat(tableParameters.size(), is(1));
    assertThat(tableParameters.get(SECOND_OVERRIDE_KEY), is(SECOND_OVERRIDE_VALUE));
  }

  private EventTableReplication createEventTableReplication(String overrideKey, String overrideValue) {
    Map<String, Object> transformOptions = new HashMap<>();
    Map<String, Object> tableParametersOptions = new HashMap<>();
    tableParametersOptions.put(overrideKey, overrideValue);
    transformOptions.put(CircusTrainTransformOptions.TABLE_REPLICATION_TABLE_PARAMETERS, tableParametersOptions);
    return createEventTableReplication(transformOptions);
  }

  private EventTableReplication createEventTableReplication(Map<String, Object> transformOptions) {
    return new EventTableReplication(null, null, null, null, transformOptions);
  }
}
