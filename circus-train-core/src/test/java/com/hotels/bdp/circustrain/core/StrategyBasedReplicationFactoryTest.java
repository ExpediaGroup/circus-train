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
package com.hotels.bdp.circustrain.core;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.Replication;
import com.hotels.bdp.circustrain.api.conf.ReplicaTable;
import com.hotels.bdp.circustrain.api.conf.ReplicationStrategy;
import com.hotels.bdp.circustrain.api.conf.SourceTable;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.event.ReplicaCatalogListener;
import com.hotels.bdp.circustrain.api.listener.HousekeepingListener;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@RunWith(MockitoJUnitRunner.class)
public class StrategyBasedReplicationFactoryTest {

  private @Mock ReplicationFactoryImpl upsertReplicationFactory;
  private @Mock Supplier<CloseableMetaStoreClient> sourceMetaStoreClientSupplier;
  private @Mock Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier;
  private @Mock HousekeepingListener housekeepingListener;
  private @Mock ReplicaCatalogListener replicaCatalogListener;
  private final TableReplication tableReplication = new TableReplication();

  @Before
  public void setUp() {
    SourceTable sourceTable = new SourceTable();
    sourceTable.setDatabaseName("db");
    sourceTable.setTableName("tableSource");
    tableReplication.setSourceTable(sourceTable);
    ReplicaTable replicaTable = new ReplicaTable();
    replicaTable.setDatabaseName("db");
    replicaTable.setTableName("tableReplica");
    tableReplication.setReplicaTable(replicaTable);
  }

  @Test
  public void newInstance() throws Exception {
    StrategyBasedReplicationFactory factory = new StrategyBasedReplicationFactory(upsertReplicationFactory,
        sourceMetaStoreClientSupplier, replicaMetaStoreClientSupplier, housekeepingListener, replicaCatalogListener);
    tableReplication.setReplicationStrategy(ReplicationStrategy.DESTRUCTIVE);
    Replication replication = factory.newInstance(tableReplication);
    assertThat(replication, instanceOf(DestructiveReplication.class));
  }

  @Test
  public void newInstanceUpsert() throws Exception {
    StrategyBasedReplicationFactory factory = new StrategyBasedReplicationFactory(upsertReplicationFactory,
        sourceMetaStoreClientSupplier, replicaMetaStoreClientSupplier, housekeepingListener, replicaCatalogListener);
    tableReplication.setReplicationStrategy(ReplicationStrategy.UPSERT);
    factory.newInstance(tableReplication);
    verify(upsertReplicationFactory).newInstance(tableReplication);
  }

}
