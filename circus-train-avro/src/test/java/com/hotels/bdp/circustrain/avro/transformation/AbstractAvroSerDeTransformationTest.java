/**
 * Copyright (C) 2016-2019 Expedia Inc.
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

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.event.EventReplicaTable;
import com.hotels.bdp.circustrain.api.event.EventTableReplication;
import com.hotels.bdp.circustrain.avro.conf.AvroSerDeConfig;

@RunWith(MockitoJUnitRunner.class)
public class AbstractAvroSerDeTransformationTest {

  private static final String DEFAULT_DESTINATION_FOLDER = "default";
  private static final String TABLE_LOCATION = "location";

  private class DummyAvroSerDeTransformation extends AbstractAvroSerDeTransformation {

    protected DummyAvroSerDeTransformation(AvroSerDeConfig avroSerDeConfig) {
      super(avroSerDeConfig);
    }

  }

  private static final String EVENT_ID = "eventId";
  private AbstractAvroSerDeTransformation transformation;

  @Before
  public void setUp() {
    AvroSerDeConfig defaultConfig = new AvroSerDeConfig();
    defaultConfig.setBaseUrl(DEFAULT_DESTINATION_FOLDER);
    transformation = new DummyAvroSerDeTransformation(defaultConfig);
  }

  @Test
  public void testOneReplicationsOverride() throws Exception {
    EventTableReplication tableReplication = mockTableReplication("overrideBaseUrl");
    runLifeCycleSuccess(tableReplication);
    assertThat(transformation.getAvroSchemaDestinationFolder(), is("overrideBaseUrl"));
    assertThat(transformation.getEventId(), is(EVENT_ID));
    assertThat(transformation.getTableLocation(), is(TABLE_LOCATION));
  }

  @Test
  public void testMultipleReplicationsOverride() throws Exception {
    EventTableReplication tableReplication = mockTableReplication("overrideBaseUrl");
    runLifeCycleSuccess(tableReplication);
    assertThat(transformation.getAvroSchemaDestinationFolder(), is("overrideBaseUrl"));
    assertThat(transformation.getEventId(), is(EVENT_ID));
    assertThat(transformation.getTableLocation(), is(TABLE_LOCATION));
    EventTableReplication tableReplication2 = mockTableReplication("overrideBaseUrl2");
    runLifeCycleSuccess(tableReplication2);
    assertThat(transformation.getAvroSchemaDestinationFolder(), is("overrideBaseUrl2"));
    assertThat(transformation.getEventId(), is(EVENT_ID));
    assertThat(transformation.getTableLocation(), is(TABLE_LOCATION));
  }

  @Test
  public void testMultipleReplicationsSecondOverrideShouldUseDefault() throws Exception {
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
  public void testOneReplicationsOverrideFailureLifecycle() throws Exception {
    EventTableReplication tableReplication = mockTableReplication("overrideBaseUrl");
    runLifeCycleFailure(tableReplication);
    assertThat(transformation.getAvroSchemaDestinationFolder(), is("overrideBaseUrl"));
    assertThat(transformation.getEventId(), is(EVENT_ID));
    assertThat(transformation.getTableLocation(), is(TABLE_LOCATION));
  }

  @Test
  public void testMultipleReplicationsOverrideFailureLifecycle() throws Exception {
    EventTableReplication tableReplication = mockTableReplication("overrideBaseUrl");
    runLifeCycleSuccess(tableReplication);
    assertThat("overrideBaseUrl", is(transformation.getAvroSchemaDestinationFolder()));
    assertThat(EVENT_ID, is(transformation.getEventId()));
    assertThat(TABLE_LOCATION, is(transformation.getTableLocation()));
    EventTableReplication tableReplication2 = mockTableReplication("overrideBaseUrl2");
    runLifeCycleFailure(tableReplication2);
    assertThat(transformation.getAvroSchemaDestinationFolder(), is("overrideBaseUrl2"));
    assertThat(transformation.getEventId(), is(EVENT_ID));
    assertThat(transformation.getTableLocation(), is(TABLE_LOCATION));
  }

  @Test
  public void testMultipleReplicationsSecondOverrideShouldUseDefaultFailureLifecycle() throws Exception {
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

  private EventTableReplication mockTableReplication(String overrideBaseUrl) {
    EventTableReplication result = Mockito.mock(EventTableReplication.class);
    Map<String, Object> transformOptions = new HashMap<>();
    if (overrideBaseUrl != null) {
      Map<String, Object> avroOverrideOptions = new HashMap<>();
      avroOverrideOptions.put(AvroSerDeConfig.TABLE_REPLICATION_OVERRIDE_BASE_URL, overrideBaseUrl);
      transformOptions.put(AvroSerDeConfig.TABLE_REPLICATION_OVERRIDE_AVRO_SERDE_OPTIONS, avroOverrideOptions);
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
