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
package com.hotels.bdp.circustrain.core.replica;

import org.apache.hadoop.hive.conf.HiveConf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.conf.ReplicaCatalog;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.event.ReplicaCatalogListener;
import com.hotels.bdp.circustrain.api.listener.HousekeepingListener;
import com.hotels.bdp.circustrain.api.metadata.TableTransformation;
import com.hotels.bdp.circustrain.core.HiveEndpointFactory;
import com.hotels.bdp.circustrain.core.replica.hive.AlterTableService;
import com.hotels.bdp.circustrain.core.replica.hive.CopyPartitionsOperation;
import com.hotels.bdp.circustrain.core.replica.hive.DropTableService;
import com.hotels.bdp.circustrain.core.replica.hive.RenameTableOperation;
import com.hotels.bdp.circustrain.core.transformation.TableParametersTransformation;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@Profile({ Modules.REPLICATION })
@Component
public class ReplicaFactory implements HiveEndpointFactory<Replica> {

  private final ReplicaCatalog replicaCatalog;
  private final HiveConf replicaHiveConf;
  private final Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier;
  private final HousekeepingListener housekeepingListener;
  private final ReplicaCatalogListener replicaCatalogListener;
  private final ReplicaTableFactoryProvider replicaTableFactoryPicker;
  private final TableParametersTransformation tableParametersTransformation;

  @Autowired
  public ReplicaFactory(
      ReplicaCatalog replicaCatalog,
      @Value("#{replicaHiveConf}") HiveConf replicaHiveConf,
      Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier,
      HousekeepingListener housekeepingListener,
      ReplicaCatalogListener replicaCatalogListener,
      ReplicaTableFactoryProvider replicaTableFactoryPicker,
      TableParametersTransformation tableParametersTransformation) {
    this.replicaCatalog = replicaCatalog;
    this.replicaHiveConf = replicaHiveConf;
    this.replicaMetaStoreClientSupplier = replicaMetaStoreClientSupplier;
    this.housekeepingListener = housekeepingListener;
    this.replicaCatalogListener = replicaCatalogListener;
    this.replicaTableFactoryPicker = replicaTableFactoryPicker;
    this.tableParametersTransformation = tableParametersTransformation;
  }

  @Override
  public Replica newInstance(TableReplication tableReplication) {
    ReplicaTableFactory replicaTableFactory = replicaTableFactoryPicker.newInstance(tableReplication);
    DropTableService dropTableService = new DropTableService(tableParametersTransformation);
    AlterTableService alterTableService = new AlterTableService(dropTableService, new CopyPartitionsOperation(),
        new RenameTableOperation(dropTableService));
    return new Replica(replicaCatalog, replicaHiveConf, replicaMetaStoreClientSupplier, replicaTableFactory,
        housekeepingListener, replicaCatalogListener, tableReplication, alterTableService);
  }
}
