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
package com.hotels.bdp.circustrain.core;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.ReplicaLocationManager;
import com.hotels.bdp.circustrain.api.SourceLocationManager;
import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.copier.CopierFactory;
import com.hotels.bdp.circustrain.api.copier.CopierFactoryManager;
import com.hotels.bdp.circustrain.api.data.DataManipulationClientFactory;
import com.hotels.bdp.circustrain.api.data.DataManipulationClientFactoryManager;
import com.hotels.bdp.circustrain.api.event.CopierListener;
import com.hotels.bdp.circustrain.api.metrics.Metrics;
import com.hotels.bdp.circustrain.core.replica.Replica;
import com.hotels.bdp.circustrain.core.replica.TableType;
import com.hotels.bdp.circustrain.core.source.Source;

@RunWith(MockitoJUnitRunner.class)
public class UnpartitionedTableReplicationTest {

  private static final String EVENT_ID = "event_id";
  private static final String TABLE = "table";
  private static final String DATABASE = "database";
  private static final String MAPPED_TABLE = "mapped_table";
  private static final String MAPPED_DATABASE = "mapped_database";

  @Mock
  private Source source;
  @Mock
  private TableAndStatistics sourceTableAndStatistics;
  @Mock
  private Table sourceTable;
  @Mock
  private Replica replica;
  @Mock
  private CopierFactoryManager copierFactoryManager;
  @Mock
  private CopierFactory copierFactory;
  @Mock
  private Copier copier;
  @Mock
  private Map<String, Object> copierOptions;
  @Mock
  private EventIdFactory eventIdFactory;
  @Mock
  private SourceLocationManager sourceLocationManager;
  @Mock
  private ReplicaLocationManager replicaLocationManager;
  @Mock
  private CopierListener listener;
  @Mock
  private DataManipulationClientFactoryManager clientFactoryManager;
  @Mock
  private DataManipulationClientFactory clientFactory;

  private final Path sourceTableLocation = new Path("sourceTableLocation");
  private final Path replicaTableLocation = new Path("replicaTableLocation");
  private final String targetTableLoation = "targetTableLocation";

  @Before
  public void injectMocks() throws Exception {
    when(eventIdFactory.newEventId(anyString())).thenReturn(EVENT_ID);
    when(source.getTableAndStatistics(DATABASE, TABLE)).thenReturn(sourceTableAndStatistics);
    when(sourceTableAndStatistics.getTable()).thenReturn(sourceTable);
    when(source.getLocationManager(sourceTable, EVENT_ID)).thenReturn(sourceLocationManager);
    when(sourceLocationManager.getTableLocation()).thenReturn(sourceTableLocation);
    when(copierFactoryManager.getCopierFactory(sourceTableLocation, replicaTableLocation, copierOptions))
        .thenReturn(copierFactory);
    when(copierFactory.newInstance(EVENT_ID, sourceTableLocation, replicaTableLocation, copierOptions))
        .thenReturn(copier);
    when(replicaLocationManager.getTableLocation()).thenReturn(replicaTableLocation);
    when(clientFactoryManager.getClientFactory(sourceTableLocation, replicaTableLocation, copierOptions))
        .thenReturn(clientFactory);
  }

  @Test
  public void typical() throws Exception {
    when(replica.getLocationManager(TableType.UNPARTITIONED, targetTableLoation, EVENT_ID, sourceLocationManager))
        .thenReturn(replicaLocationManager);
    UnpartitionedTableReplication replication = new UnpartitionedTableReplication(DATABASE, TABLE, source, replica,
        copierFactoryManager, eventIdFactory, targetTableLoation, DATABASE, TABLE, copierOptions, listener,
        clientFactoryManager);
    replication.replicate();

    InOrder replicationOrder = inOrder(copierFactoryManager, copierFactory, copier, sourceLocationManager, replica,
        replicaLocationManager, listener);
    replicationOrder.verify(replica).validateReplicaTable(DATABASE, TABLE);
    replicationOrder
        .verify(copierFactoryManager)
        .getCopierFactory(sourceTableLocation, replicaTableLocation, copierOptions);
    replicationOrder
        .verify(copierFactory)
        .newInstance(EVENT_ID, sourceTableLocation, replicaTableLocation, copierOptions);
    replicationOrder.verify(listener).copierStart(anyString());
    replicationOrder.verify(copier).copy();
    replicationOrder.verify(listener).copierEnd(any(Metrics.class));
    replicationOrder.verify(sourceLocationManager).cleanUpLocations();
    replicationOrder
        .verify(replica)
        .updateMetadata(EVENT_ID, sourceTableAndStatistics, DATABASE, TABLE, replicaLocationManager);
    replicationOrder.verify(replicaLocationManager).cleanUpLocations();
  }

  @Test
  public void mappedNames() throws Exception {
    when(replica.getLocationManager(TableType.UNPARTITIONED, targetTableLoation, EVENT_ID, sourceLocationManager))
        .thenReturn(replicaLocationManager);

    UnpartitionedTableReplication replication = new UnpartitionedTableReplication(DATABASE, TABLE, source, replica,
        copierFactoryManager, eventIdFactory, targetTableLoation, MAPPED_DATABASE, MAPPED_TABLE, copierOptions,
        listener, clientFactoryManager);
    replication.replicate();

    InOrder replicationOrder = inOrder(copierFactoryManager, copierFactory, copier, sourceLocationManager, replica,
        replicaLocationManager);
    replicationOrder.verify(replica).validateReplicaTable(MAPPED_DATABASE, MAPPED_TABLE);
    replicationOrder
        .verify(copierFactoryManager)
        .getCopierFactory(sourceTableLocation, replicaTableLocation, copierOptions);
    replicationOrder
        .verify(copierFactory)
        .newInstance(EVENT_ID, sourceTableLocation, replicaTableLocation, copierOptions);
    replicationOrder.verify(copier).copy();
    replicationOrder.verify(sourceLocationManager).cleanUpLocations();
    replicationOrder
        .verify(replica)
        .updateMetadata(EVENT_ID, sourceTableAndStatistics, MAPPED_DATABASE, MAPPED_TABLE, replicaLocationManager);
    replicationOrder.verify(replicaLocationManager).cleanUpLocations();
  }

  @Test
  public void copierListenerCalledWhenException() throws Exception {
    when(replica.getLocationManager(TableType.UNPARTITIONED, targetTableLoation, EVENT_ID, sourceLocationManager))
        .thenReturn(replicaLocationManager);

    when(copier.copy()).thenThrow(new CircusTrainException("copy failed"));

    UnpartitionedTableReplication replication = new UnpartitionedTableReplication(DATABASE, TABLE, source, replica,
        copierFactoryManager, eventIdFactory, targetTableLoation, DATABASE, TABLE, copierOptions, listener,
        clientFactoryManager);
    try {
      replication.replicate();
      fail("Copy exception should be caught and rethrown");
    } catch (CircusTrainException e) {
      InOrder replicationOrder = inOrder(copier, listener);
      replicationOrder.verify(listener).copierStart(anyString());
      replicationOrder.verify(copier).copy();
      // Still called
      replicationOrder.verify(listener).copierEnd(any(Metrics.class));
    }
  }
}
