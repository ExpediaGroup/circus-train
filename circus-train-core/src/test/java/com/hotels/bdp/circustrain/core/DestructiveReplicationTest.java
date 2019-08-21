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
package com.hotels.bdp.circustrain.core;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.Replication;
import com.hotels.bdp.circustrain.api.conf.ReplicaTable;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.core.replica.DestructiveReplica;
import com.hotels.bdp.circustrain.core.annotation.HiveTableAnnotator;
import com.hotels.bdp.circustrain.core.source.DestructiveSource;

@RunWith(MockitoJUnitRunner.class)
public class DestructiveReplicationTest {

  private static final String DATABASE = "db";
  private static final String REPLICA_TABLE = "table2";
  private static final String EVENT_ID = "eventId";

  private final TableReplication tableReplication = new TableReplication();
  private @Mock ReplicationFactoryImpl upsertReplicationFactory;
  private @Mock DestructiveSource destructiveSource;
  private @Mock DestructiveReplica destructiveReplica;
  private @Mock Replication normalReplication;
  private @Mock HiveTableAnnotator hiveTableAnnotator;
  private final List<String> sourcePartitionNames = Lists.newArrayList("a=b");
  private DestructiveReplication replication;

  @Before
  public void setUp() {
    ReplicaTable replicaTable = new ReplicaTable();
    replicaTable.setDatabaseName(DATABASE);
    replicaTable.setTableName(REPLICA_TABLE);
    tableReplication.setReplicaTable(replicaTable);
    replication = new DestructiveReplication(upsertReplicationFactory, tableReplication, EVENT_ID, destructiveSource,
        destructiveReplica, hiveTableAnnotator);
  }

  @Test
  public void replicateTableExists() throws Exception {
    when(destructiveReplica.tableIsUnderCircusTrainControl()).thenReturn(true);
    when(destructiveSource.tableExists()).thenReturn(true);
    when(upsertReplicationFactory.newInstance(tableReplication)).thenReturn(normalReplication);
    when(destructiveSource.getPartitionNames()).thenReturn(sourcePartitionNames);

    replication.replicate();
    verify(destructiveReplica).dropDeletedPartitions(sourcePartitionNames);
    verify(normalReplication).replicate();
    verify(hiveTableAnnotator).annotateTable(DATABASE, REPLICA_TABLE, new HashMap<String, String>());
  }

  @Test
  public void replicateTableDoesNotExists() throws Exception {
    when(destructiveReplica.tableIsUnderCircusTrainControl()).thenReturn(true);
    when(destructiveSource.tableExists()).thenReturn(false);

    replication.replicate();
    verifyZeroInteractions(upsertReplicationFactory);
    verify(destructiveReplica).dropTable();
  }

  @Test(expected = CircusTrainException.class)
  public void replicateNotCircusTrainTable() throws Exception {
    when(destructiveReplica.tableIsUnderCircusTrainControl()).thenReturn(false);

    replication.replicate();
  }

  @Test(expected = CircusTrainException.class)
  public void replicateExceptionsWrapped() throws Exception {
    when(destructiveReplica.tableIsUnderCircusTrainControl()).thenThrow(new TException());

    replication.replicate();
  }

  @Test
  public void name() throws Exception {
    DestructiveReplication replication = new DestructiveReplication(upsertReplicationFactory, tableReplication,
        EVENT_ID, destructiveSource, destructiveReplica, hiveTableAnnotator);
    assertThat(replication.name(), is("destructive-db.table2"));
  }

  @Test
  public void getEventId() throws Exception {
    DestructiveReplication replication = new DestructiveReplication(upsertReplicationFactory, tableReplication,
        EVENT_ID, destructiveSource, destructiveReplica, hiveTableAnnotator);
    assertThat(replication.getEventId(), is(EVENT_ID));
  }

}
