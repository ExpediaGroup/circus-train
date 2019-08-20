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
package com.hotels.bdp.circustrain.core.replica;

import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.conf.OrphanedDataOptions;
import com.hotels.bdp.circustrain.core.transformation.PropertiesTableTransformation;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class BeekeeperEventBasedCleanup implements EventBasedCleanup {

  private static final String BEEKEEPER_PARAM_KEY = "beekeeper.remove.unreferenced.data";
  private static final String BEEKEEPER_PARAM_VALUE_ENABLE = "true";
  private static final String BEEKEEPER_PARAM_VALUE_DISABLE = "false";
  private Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier;
  private OrphanedDataOptions orphanedDataOptions;

  public BeekeeperEventBasedCleanup(
    Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier,
    OrphanedDataOptions orphanedDataOptions) {
    this.replicaMetaStoreClientSupplier = replicaMetaStoreClientSupplier;
    this.orphanedDataOptions = orphanedDataOptions;
  }

  @Override
  public void enableEventBasedCleanup(Table table) throws CircusTrainException {
    PropertiesTableTransformation beekeeperTableTransformation = new PropertiesTableTransformation(
      replicaMetaStoreClientSupplier, getBeekeeperProperties());
    beekeeperTableTransformation.transform(table);
  }

  @Override
  public void disableEventBasedCleanup(Table table) {
    PropertiesTableTransformation beekeeperTableTransformation = new PropertiesTableTransformation(
      replicaMetaStoreClientSupplier, Collections.singletonMap(BEEKEEPER_PARAM_KEY, BEEKEEPER_PARAM_VALUE_ENABLE));
    beekeeperTableTransformation.transform(table);
  }

  private Map<String, String> getBeekeeperProperties() {
    Map<String, String> tableProperties = Collections.singletonMap(BEEKEEPER_PARAM_KEY,
      BEEKEEPER_PARAM_VALUE_DISABLE);
    tableProperties.putAll(orphanedDataOptions.getOrphanedDataOptions());
    return tableProperties;
  }
}
