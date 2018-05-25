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

import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;

public class MetaStoreClientFactoryManagerTest {

  @Test
  public void factoryForThrift() {
    List<ConditionalMetaStoreClientFactory> list = Collections
        .<ConditionalMetaStoreClientFactory> singletonList(
            new ThriftHiveMetaStoreClientFactory(new HiveConf(), "name"));
    MetaStoreClientFactoryManager factoryManager = new MetaStoreClientFactoryManager(list);
    MetaStoreClientFactory clientFactory = factoryManager.factoryForUrl(
        ThriftHiveMetaStoreClientFactory.ACCEPT_PREFIX);
    assertTrue(clientFactory instanceof ThriftHiveMetaStoreClientFactory);
  }

  @Test(expected = RuntimeException.class)
  public void factoryForUnsupportedUrl() {
    List<ConditionalMetaStoreClientFactory> list = Collections.emptyList();
    MetaStoreClientFactoryManager factoryManager = new MetaStoreClientFactoryManager(list);
    factoryManager.factoryForUrl("unsupported:///bla");
  }

}
