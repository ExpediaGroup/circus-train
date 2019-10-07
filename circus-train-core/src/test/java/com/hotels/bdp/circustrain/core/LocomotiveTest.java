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
package com.hotels.bdp.circustrain.core;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.boot.ApplicationArguments;

import com.hotels.bdp.circustrain.api.CompletionCode;
import com.hotels.bdp.circustrain.api.Replication;
import com.hotels.bdp.circustrain.api.conf.ReplicaCatalog;
import com.hotels.bdp.circustrain.api.conf.ReplicaTable;
import com.hotels.bdp.circustrain.api.conf.Security;
import com.hotels.bdp.circustrain.api.conf.SourceCatalog;
import com.hotels.bdp.circustrain.api.conf.SourceTable;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.conf.TableReplications;
import com.hotels.bdp.circustrain.api.event.EventReplicaCatalog;
import com.hotels.bdp.circustrain.api.event.EventSourceCatalog;
import com.hotels.bdp.circustrain.api.event.EventTableReplication;
import com.hotels.bdp.circustrain.api.event.LocomotiveListener;
import com.hotels.bdp.circustrain.api.event.TableReplicationListener;
import com.hotels.bdp.circustrain.api.metrics.MetricSender;

@RunWith(MockitoJUnitRunner.class)
public class LocomotiveTest {

  private @Mock Security security;
  private @Mock SourceCatalog sourceCatalog;
  private @Mock ReplicaCatalog replicaCatalog;
  private @Mock SourceTable sourceTable;
  private @Mock ReplicaTable replicaTable;
  private @Mock TableReplication tableReplication1;
  private @Mock TableReplication tableReplication2;
  private @Mock TableReplications tableReplications;
  private @Mock ReplicationFactory replicationFactory;
  private @Mock Replication replication1;
  private @Mock Replication replication2;
  private @Mock ApplicationArguments applicationArguments;

  private Locomotive locomotive;

  @Before
  public void buildConfig() {
    when(sourceCatalog.getName()).thenReturn("source-catalog");
    when(replicaCatalog.getName()).thenReturn("replica-catalog");
    when(sourceTable.getQualifiedName()).thenReturn("source-database.source-table");
    when(tableReplication1.getQualifiedReplicaName()).thenReturn("replica-database.replica-table1");
    when(tableReplication1.getSourceTable()).thenReturn(sourceTable);
    when(tableReplication1.getReplicaTable()).thenReturn(replicaTable);
    when(tableReplication2.getQualifiedReplicaName()).thenReturn("replica-database.replica-table2");
    when(tableReplication2.getSourceTable()).thenReturn(sourceTable);
    when(tableReplication2.getReplicaTable()).thenReturn(replicaTable);
    when(tableReplications.getTableReplications()).thenReturn(Arrays.asList(tableReplication1, tableReplication2));

    when(replicationFactory.newInstance(tableReplication1)).thenReturn(replication1);
    when(replicationFactory.newInstance(tableReplication2)).thenReturn(replication2);

    locomotive = new Locomotive(sourceCatalog, replicaCatalog, security, tableReplications, replicationFactory,
        MetricSender.DEFAULT_LOG_ONLY, new LocomotiveListener() {

          @Override
          public void circusTrainStartUp(
              String[] args,
              EventSourceCatalog sourceCatalog,
              EventReplicaCatalog replicaCatalog) {}

          @Override
          public void circusTrainShutDown(CompletionCode completionCode, Map<String, Long> metrics) {}
        }, new TableReplicationListener() {

          @Override
          public void tableReplicationSuccess(EventTableReplication eventTableReplication, String eventId) {}

          @Override
          public void tableReplicationStart(EventTableReplication tableReplication, String eventId) {}

          @Override
          public void tableReplicationFailure(
              EventTableReplication eventTableReplication,
              String eventId,
              Throwable t) {}
        });
  }

  @Test
  public void getReplicationSummary() {
    assertThat(locomotive.getReplicationSummary(tableReplication1),
        is("source-catalog:source-database.source-table to replica-catalog:replica-database.replica-table1"));
  }

  @Test
  public void exitCodeIsZeroWhenAllReplicationsAreSuccessful() {
    locomotive.run(applicationArguments);
    assertThat(locomotive.getExitCode(), is(0));
  }

  @Test
  public void exitCodeIsMinusOneWhenAllReplicationsFail() {
    doThrow(new RuntimeException()).when(replication1).replicate();
    doThrow(new RuntimeException()).when(replication2).replicate();
    locomotive.run(applicationArguments);
    assertThat(locomotive.getExitCode(), is(-1));
  }

  @Test
  public void exitCodeIsMinusTwoWhenOneReplicationFails() {
    doThrow(new RuntimeException()).when(replication2).replicate();
    locomotive.run(applicationArguments);
    assertThat(locomotive.getExitCode(), is(-2));
  }

}
