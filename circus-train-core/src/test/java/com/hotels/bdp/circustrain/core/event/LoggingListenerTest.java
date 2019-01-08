/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
package com.hotels.bdp.circustrain.core.event;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.event.EventPartition;
import com.hotels.bdp.circustrain.api.event.EventPartitions;
import com.hotels.bdp.circustrain.api.event.EventReplicaCatalog;
import com.hotels.bdp.circustrain.api.event.EventSourceCatalog;
import com.hotels.bdp.circustrain.api.event.EventSourceTable;
import com.hotels.bdp.circustrain.api.event.EventTableReplication;
import com.hotels.bdp.circustrain.core.event.LoggingListener.ReplicationState;

@RunWith(MockitoJUnitRunner.class)
public class LoggingListenerTest {

  @Mock
  private EventPartition eventPartition;
  @Mock
  private EventTableReplication tableReplication;
  @Mock
  private EventSourceCatalog sourceCatalog;
  @Mock
  private EventReplicaCatalog replicaCatalog;
  @Mock
  private EventSourceTable eventSourceTable;

  private final LoggingListener listener = new LoggingListener();

  @Before
  public void setUp() {
    listener.circusTrainStartUp(new String[] {}, sourceCatalog, replicaCatalog);
    when(tableReplication.getSourceTable()).thenReturn(eventSourceTable);
  }

  @Test
  public void failureWithoutStartOrSuccessDoesntThrowException() {
    Throwable throwable = new Throwable("Test");
    listener.tableReplicationFailure(tableReplication, "event-id", throwable);
  }

  @Test
  public void resetAlteredPartitionsCount() {
    EventPartitions eventPartitions = new EventPartitions(new LinkedHashMap<String, String>());
    eventPartitions.add(eventPartition);

    listener.tableReplicationStart(tableReplication, "event-id");
    listener.partitionsToAlter(eventPartitions);
    listener.partitionsToCreate(eventPartitions);
    assertThat(listener.getReplicationState().partitionsAltered, is(2));

    // state should be reset
    listener.tableReplicationStart(tableReplication, "event-id");
    assertThat(listener.getReplicationState().partitionsAltered, is(0));
  }

  @Test
  public void tableReplicationStartResetsReplicationState() {
    ReplicationState replicationState1 = listener.getReplicationState();
    listener.tableReplicationStart(tableReplication, "event-id");
    ReplicationState replicationState2 = listener.getReplicationState();
    assertThat(replicationState1, not(is(replicationState2)));
  }

}
