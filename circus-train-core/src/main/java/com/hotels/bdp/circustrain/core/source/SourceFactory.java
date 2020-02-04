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
package com.hotels.bdp.circustrain.core.source;

import org.apache.hadoop.hive.conf.HiveConf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.bdp.circustrain.api.conf.SourceCatalog;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.event.SourceCatalogListener;
import com.hotels.bdp.circustrain.core.HiveEndpointFactory;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@Profile({ Modules.REPLICATION })
@Component
public class SourceFactory implements HiveEndpointFactory<Source> {

  private final SourceCatalog sourceCatalog;
  private final HiveConf sourceHiveConf;
  private final Supplier<CloseableMetaStoreClient> sourceMetaStoreClientSupplier;
  private final SourceCatalogListener sourceCatalogListener;

  @Autowired
  public SourceFactory(
      SourceCatalog sourceCatalog,
      @Value("#{sourceHiveConf}") HiveConf sourceHiveConf,
      @Value("#{sourceMetaStoreClientSupplier}") Supplier<CloseableMetaStoreClient> sourceMetaStoreClientSupplier,
      SourceCatalogListener sourceCatalogListener) {
    this.sourceCatalog = sourceCatalog;
    this.sourceHiveConf = sourceHiveConf;
    this.sourceMetaStoreClientSupplier = sourceMetaStoreClientSupplier;
    this.sourceCatalogListener = sourceCatalogListener;
  }

  @Override
  public Source newInstance(TableReplication tableReplication) {
    // disable snapshots by default, only needed for ReplicationModes that replicate data.
    boolean snapshotsDisabled = true;
    if (tableReplication.getReplicationMode() == ReplicationMode.FULL) {
      snapshotsDisabled = sourceCatalog.isDisableSnapshots();
    }
    return new Source(sourceCatalog, sourceHiveConf, sourceMetaStoreClientSupplier, sourceCatalogListener,
        snapshotsDisabled, tableReplication.getSourceTable().getTableLocation());
  }

}
