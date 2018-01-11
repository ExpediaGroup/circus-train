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
package com.hotels.bdp.circustrain.core.replica;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.event.ReplicaCatalogListener;
import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;
import com.hotels.bdp.circustrain.core.conf.ReplicaCatalog;
import com.hotels.bdp.circustrain.core.conf.TableReplication;
import com.hotels.bdp.circustrain.api.listener.HousekeepingListener;

@RunWith(MockitoJUnitRunner.class)
public class ReplicaFactoryTest {

  @Mock
  private ReplicaCatalog replicaCatalog;
  @Mock
  private HiveConf replicaHiveConf;
  @Mock
  private Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier;
  @Mock
  private HousekeepingListener housekeepingListener;
  @Mock
  private ReplicaCatalogListener replicaCatalogListener;
  @Mock
  private ReplicaTableFactoryProvider replicaTableFactoryPicker;
  @Mock
  private TableReplication tableReplication;

  private ReplicaFactory replicaFactory;

  @Before
  public void setUp() {
    replicaFactory = new ReplicaFactory(replicaCatalog, replicaHiveConf, replicaMetaStoreClientSupplier,
        housekeepingListener, replicaCatalogListener, replicaTableFactoryPicker);
  }

  @Test
  public void defaultReplicaTableFactory() throws Exception {
    Replica replica = replicaFactory.newInstance(tableReplication);
    assertNotNull(replica);
    verify(replicaTableFactoryPicker).newInstance(tableReplication);
  }

}
