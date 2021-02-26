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
package com.hotels.bdp.circustrain.core;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.ReplicaLocationManager;
import com.hotels.bdp.circustrain.api.SourceLocationManager;
import com.hotels.bdp.circustrain.core.replica.Replica;
import com.hotels.bdp.circustrain.core.source.Source;

@RunWith(MockitoJUnitRunner.class)
public class UnpartitionedTableMetadataMirrorReplicationTest {

  private static final String EVENT_ID = "event_id";
  private static final String TABLE = "table";
  private static final String DATABASE = "database";

  @Mock
  private Source source;
  @Mock
  private TableAndStatistics sourceTableAndStatistics;
  @Mock
  private Table sourceTable;
  @Mock
  private Replica replica;
  @Mock
  private EventIdFactory eventIdFactory;
  @Mock
  private SourceLocationManager sourceLocationManager;

  @Before
  public void injectMocks() throws Exception {
    when(eventIdFactory.newEventId(anyString())).thenReturn(EVENT_ID);
    when(source.getTableAndStatistics(DATABASE, TABLE)).thenReturn(sourceTableAndStatistics);
    when(sourceTableAndStatistics.getTable()).thenReturn(sourceTable);
    when(source.getLocationManager(sourceTable, EVENT_ID)).thenReturn(sourceLocationManager);
  }

  @Test
  public void typical() throws Exception {
    UnpartitionedTableMetadataMirrorReplication replication = new UnpartitionedTableMetadataMirrorReplication(DATABASE,
        TABLE, source, replica, eventIdFactory, DATABASE, TABLE);
    replication.replicate();

    InOrder replicationOrder = inOrder(sourceLocationManager, replica);
    replicationOrder.verify(replica).validateReplicaTable(DATABASE, TABLE);
    replicationOrder.verify(sourceLocationManager).cleanUpLocations();
    replicationOrder.verify(replica).updateMetadata(eq(EVENT_ID), eq(sourceTableAndStatistics), eq(DATABASE), eq(TABLE),
        any(ReplicaLocationManager.class));
  }

}
