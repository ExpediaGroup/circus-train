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
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.circustrain.avro.TestUtils.newTable;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.event.EventReplicaTable;
import com.hotels.bdp.circustrain.api.event.EventTableReplication;
import com.hotels.bdp.circustrain.avro.conf.AvroSerDeConfig;
import com.hotels.bdp.circustrain.avro.hive.HiveObjectUtils;
import com.hotels.bdp.circustrain.avro.util.SchemaCopier;

@RunWith(MockitoJUnitRunner.class)
public class AvroSerDeTableTransformationTest {

  private static final String AVRO_SCHEMA_URL_PARAMETER = "avro.schema.url";

  @Mock
  private AvroSerDeConfig avroSerDeConfig;

  @Mock
  private SchemaCopier schemaCopier;

  @Mock
  private EventTableReplication tableReplicationEvent;

  private AvroSerDeTableTransformation transformation;
  private final Table table = newTable();
  private final Path destinationPath = new Path("/destination/path");
  private final String destinationPathString = destinationPath.toString();

  @Before
  public void setUp() {
    transformation = new AvroSerDeTableTransformation(avroSerDeConfig, schemaCopier);
  }

  @Test
  public void transformNoAvro() {
    transformation.transform(table);
    verifyZeroInteractions(schemaCopier);
    assertThat(table, is(newTable()));
  }

  @Test
  public void missingEventId() {
    when(avroSerDeConfig.getBaseUrl()).thenReturn("schema");
    transformation.transform(table);
    verifyZeroInteractions(schemaCopier);
    assertThat(table, is(newTable()));
  }

  @Test
  public void missingAvroDestinationFolder() {
    EventReplicaTable eventReplicaTable = new EventReplicaTable("db", "table", "location");
    when(tableReplicationEvent.getReplicaTable()).thenReturn(eventReplicaTable);
    transformation.tableReplicationStart(tableReplicationEvent, "eventId");

    transformation.transform(table);
    verifyZeroInteractions(schemaCopier);
    assertThat(table, is(newTable()));
  }

  @Test
  public void transformNoSourceUrl() throws Exception {
    when(avroSerDeConfig.getBaseUrl()).thenReturn("schema");
    EventReplicaTable eventReplicaTable = new EventReplicaTable("db", "table", "location");
    when(tableReplicationEvent.getReplicaTable()).thenReturn(eventReplicaTable);
    transformation.tableReplicationStart(tableReplicationEvent, "eventId");

    Table result = transformation.transform(table);
    verifyZeroInteractions(schemaCopier);
    assertThat(result, is(newTable()));
  }

  @Test
  public void transformNoTargetSchemaLocationSpecifiedUseDefault() throws Exception {
    EventReplicaTable eventReplicaTable = new EventReplicaTable("db", "table", "location");
    when(tableReplicationEvent.getReplicaTable()).thenReturn(eventReplicaTable);
    HiveObjectUtils.updateSerDeUrl(table, AVRO_SCHEMA_URL_PARAMETER, "avroSourceUrl");
    when(schemaCopier.copy("avroSourceUrl", "location/eventId/.schema")).thenReturn(destinationPath);

    transformation.tableReplicationStart(tableReplicationEvent, "eventId");
    Table result = transformation.transform(table);
    assertThat(result.getParameters().get(AVRO_SCHEMA_URL_PARAMETER), is(destinationPathString));
  }

  @Test
  public void transform() throws Exception {
    when(avroSerDeConfig.getBaseUrl()).thenReturn("schema");
    EventReplicaTable eventReplicaTable = new EventReplicaTable("db", "table", "location");
    when(tableReplicationEvent.getReplicaTable()).thenReturn(eventReplicaTable);
    HiveObjectUtils.updateSerDeUrl(table, AVRO_SCHEMA_URL_PARAMETER, "avroSourceUrl");
    when(schemaCopier.copy("avroSourceUrl", "schema/eventId/")).thenReturn(destinationPath);

    transformation.tableReplicationStart(tableReplicationEvent, "eventId");
    Table result = transformation.transform(table);
    assertThat(result.getParameters().get(AVRO_SCHEMA_URL_PARAMETER), is(destinationPathString));
  }

  @Test
  public void transformOverride() throws Exception {
    when(avroSerDeConfig.getBaseUrl()).thenReturn("schema");
    Map<String, Object> avroOverrideOptions = new HashMap<>();
    avroOverrideOptions.put(AvroSerDeConfig.TABLE_REPLICATION_OVERRIDE_BASE_URL, "schemaOverride");
    Map<String, Object> transformOptions = new HashMap<>();
    transformOptions.put(AvroSerDeConfig.TABLE_REPLICATION_OVERRIDE_AVRO_SERDE_OPTIONS, avroOverrideOptions);
    when(tableReplicationEvent.getTransformOptions()).thenReturn(transformOptions);
    EventReplicaTable eventReplicaTable = new EventReplicaTable("db", "table", "location");
    when(tableReplicationEvent.getReplicaTable()).thenReturn(eventReplicaTable);
    transformation.tableReplicationStart(tableReplicationEvent, "eventId");

    HiveObjectUtils.updateSerDeUrl(table, AVRO_SCHEMA_URL_PARAMETER, "avroSourceUrl");
    when(schemaCopier.copy("avroSourceUrl", "schemaOverride/eventId/")).thenReturn(destinationPath);

    Table result = transformation.transform(table);
    assertThat(result.getParameters().get(AVRO_SCHEMA_URL_PARAMETER), is(destinationPathString));
  }

}
