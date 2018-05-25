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
package com.hotels.bdp.circustrain.core.metastore;

import java.util.List;

import com.hotels.hcommon.hive.metastore.MetaStoreClientException;
import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;

public class MetaStoreClientFactoryManager {
  private final List<ConditionalMetaStoreClientFactory> metaStoreClientFactories;

  public MetaStoreClientFactoryManager(List<ConditionalMetaStoreClientFactory> metaStoreClientFactories) {
    this.metaStoreClientFactories = metaStoreClientFactories;
  }

  public MetaStoreClientFactory factoryForUrl(String url) {
    for (ConditionalMetaStoreClientFactory metaStoreClientFactory : metaStoreClientFactories) {
      if (metaStoreClientFactory.accepts(url)) {
        return metaStoreClientFactory;
      }
    }
    throw new MetaStoreClientException("No MetaStoreClientFactory found for url " + url);
  }
}
