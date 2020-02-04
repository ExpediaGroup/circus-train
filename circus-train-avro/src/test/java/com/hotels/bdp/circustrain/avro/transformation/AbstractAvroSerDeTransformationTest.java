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
package com.hotels.bdp.circustrain.avro.transformation;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.circustrain.avro.conf.AvroSerDeConfig.AVRO_SERDE_OPTIONS;
import static com.hotels.bdp.circustrain.avro.conf.AvroSerDeConfig.BASE_URL;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.conf.TransformOptions;
import com.hotels.bdp.circustrain.api.event.EventReplicaTable;
import com.hotels.bdp.circustrain.api.event.EventTableReplication;
import com.hotels.bdp.circustrain.avro.conf.AvroSerDeConfig;

@RunWith(MockitoJUnitRunner.class)
public class AbstractAvroSerDeTransformationTest {

  private static final String DEFAULT_DESTINATION_FOLDER = "default";
  private static final String TABLE_LOCATION = "location";

  private class DummyAvroSerDeTransformation extends AbstractAvroSerDeTransformation {

    protected DummyAvroSerDeTransformation(TransformOptions transformOptions) {
      super(transformOptions);
    }

  }

  private static final String EVENT_ID = "eventId";
  private AbstractAvroSerDeTransformation transformation;

  @Before
  public void setUp() {
    Map<String, String> avroSerdeOptions = new HashMap<>();
    avroSerdeOptions.put(BASE_URL, DEFAULT_DESTINATION_FOLDER);
    Map<String, Object> options = new HashMap<>();
    options.put(AVRO_SERDE_OPTIONS, avroSerdeOptions);
    TransformOptions transformOptions = new TransformOptions();
    transformOptions.setTransformOptions(options);
    transformation = new DummyAvroSerDeTransformation(transformOptions);
  }

  @Test
  public void testOneReplicationsOverride() {
    EventTableReplication tableReplication = mockTableReplication("overrideBaseUrl");
    runLifeCycleSuccess(tableReplication);
    assertThat(transformation.getAvroSchemaDestinationFolder(), is("overrideBaseUrl"));
    assertThat(transformation.getEventId(), is(EVENT_ID));
    assertThat(transformation.getTableReplication(), is(tableReplication));
    assertThat(transformation.getTableLocation(), is(TABLE_LOCATION));
  }

  @Test
  public void testMultipleReplicationsOverride() {
    EventTableReplication tableReplication = mockTableReplication("overrideBaseUrl");
    runLifeCycleSuccess(tableReplication);
    assertThat(transformation.getAvroSchemaDestinationFolder(), is("overrideBaseUrl"));
    assertThat(transformation.getEventId(), is(EVENT_ID));
    assertThat(transformation.getTableReplication(), is(tableReplication));
    assertThat(transformation.getTableLocation(), is(TABLE_LOCATION));
    EventTableReplication tableReplication2 = mockTableReplication("overrideBaseUrl2");
    runLifeCycleSuccess(tableReplication2);
    assertThat(transformation.getAvroSchemaDestinationFolder(), is("overrideBaseUrl2"));
    assertThat(transformation.getEventId(), is(EVENT_ID));
    assertThat(transformation.getTableReplication(), is(tableReplication2));
    assertThat(transformation.getTableLocation(), is(TABLE_LOCATION));
  }

  @Test
  public void testMultipleReplicationsSecondOverrideShouldUseDefault() {
    EventTableReplication tableReplication = mockTableReplication("overrideBaseUrl");
    runLifeCycleSuccess(tableReplication);
    assertThat(transformation.getAvroSchemaDestinationFolder(), is("overrideBaseUrl"));
    assertThat(transformation.getEventId(), is(EVENT_ID));
    assertThat(transformation.getTableLocation(), is(TABLE_LOCATION));
    EventTableReplication tableReplication2 = mockTableReplication(null);
    runLifeCycleSuccess(tableReplication2);
    assertThat(transformation.getAvroSchemaDestinationFolder(), is(DEFAULT_DESTINATION_FOLDER));
    assertThat(transformation.getEventId(), is(EVENT_ID));
    assertThat(transformation.getTableLocation(), is(TABLE_LOCATION));
  }

  @Test
  public void testOneReplicationsOverrideFailureLifecycle() {
    EventTableReplication tableReplication = mockTableReplication("overrideBaseUrl");
    runLifeCycleFailure(tableReplication);
    assertThat(transformation.getAvroSchemaDestinationFolder(), is("overrideBaseUrl"));
    assertThat(transformation.getEventId(), is(EVENT_ID));
    assertThat(transformation.getTableLocation(), is(TABLE_LOCATION));
    assertThat(transformation.getTableReplication(), is(tableReplication));
  }

  @Test
  public void testMultipleReplicationsOverrideFailureLifecycle() {
    EventTableReplication tableReplication = mockTableReplication("overrideBaseUrl");
    runLifeCycleSuccess(tableReplication);
    assertThat("overrideBaseUrl", is(transformation.getAvroSchemaDestinationFolder()));
    assertThat(EVENT_ID, is(transformation.getEventId()));
    assertThat(transformation.getTableReplication(), is(tableReplication));
    assertThat(TABLE_LOCATION, is(transformation.getTableLocation()));
    EventTableReplication tableReplication2 = mockTableReplication("overrideBaseUrl2");
    runLifeCycleFailure(tableReplication2);
    assertThat(transformation.getAvroSchemaDestinationFolder(), is("overrideBaseUrl2"));
    assertThat(transformation.getEventId(), is(EVENT_ID));
    assertThat(transformation.getTableReplication(), is(tableReplication2));
    assertThat(transformation.getTableLocation(), is(TABLE_LOCATION));
  }

  @Test
  public void testMultipleReplicationsSecondOverrideShouldUseDefaultFailureLifecycle() {
    EventTableReplication tableReplication = mockTableReplication("overrideBaseUrl");
    runLifeCycleSuccess(tableReplication);
    assertThat("overrideBaseUrl", is(transformation.getAvroSchemaDestinationFolder()));
    assertThat(EVENT_ID, is(transformation.getEventId()));
    assertThat(TABLE_LOCATION, is(transformation.getTableLocation()));
    EventTableReplication tableReplication2 = mockTableReplication(null);
    runLifeCycleFailure(tableReplication2);
    assertThat(transformation.getAvroSchemaDestinationFolder(), is(DEFAULT_DESTINATION_FOLDER));
    assertThat(transformation.getEventId(), is(EVENT_ID));
    assertThat(transformation.getTableLocation(), is(TABLE_LOCATION));
  }

  @Test
  public void testReplicationOverrideNullTransformOptions() {
    transformation = new DummyAvroSerDeTransformation(new TransformOptions());
    EventTableReplication tableReplication = mockTableReplication("overrideBaseUrl");
    runLifeCycleSuccess(tableReplication);
    assertThat(EVENT_ID, is(transformation.getEventId()));
    assertThat(transformation.getEventId(), is(EVENT_ID));
    assertThat(transformation.getTableLocation(), is(TABLE_LOCATION));
    assertThat(transformation.getAvroSchemaDestinationFolder(), is("overrideBaseUrl"));
  }

  @Test
  public void testNullTransformOptions() {
    transformation = new DummyAvroSerDeTransformation(new TransformOptions());
    EventTableReplication tableReplication = mockTableReplication(null);
    runLifeCycleSuccess(tableReplication);
    assertThat(EVENT_ID, is(transformation.getEventId()));
    assertThat(transformation.getEventId(), is(EVENT_ID));
    assertThat(transformation.getTableLocation(), is(TABLE_LOCATION));
    assertThat(transformation.getAvroSchemaDestinationFolder(), is(TABLE_LOCATION));
  }

  private EventTableReplication mockTableReplication(String overrideBaseUrl) {
    EventTableReplication result = Mockito.mock(EventTableReplication.class);
    Map<String, Object> transformOptions = new HashMap<>();
    if (overrideBaseUrl != null) {
      Map<String, Object> avroOverrideOptions = new HashMap<>();
      avroOverrideOptions.put(AvroSerDeConfig.BASE_URL, overrideBaseUrl);
      transformOptions.put(AvroSerDeConfig.AVRO_SERDE_OPTIONS, avroOverrideOptions);
    }
    when(result.getTransformOptions()).thenReturn(transformOptions);
    EventReplicaTable eventReplicaTable = new EventReplicaTable("db", "table", TABLE_LOCATION);
    when(result.getReplicaTable()).thenReturn(eventReplicaTable);
    return result;
  }

  private void runLifeCycleSuccess(EventTableReplication tableReplication) {
    transformation.tableReplicationStart(tableReplication, EVENT_ID);
    transformation.tableReplicationSuccess(tableReplication, EVENT_ID);
  }

  private void runLifeCycleFailure(EventTableReplication tableReplication) {
    transformation.tableReplicationStart(tableReplication, EVENT_ID);
    transformation.tableReplicationFailure(tableReplication, EVENT_ID, new Exception());
  }

}
