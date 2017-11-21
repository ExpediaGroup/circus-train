/**
 * Copyright (C) 2016-2017 Expedia Inc.
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

import org.junit.Test;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.metastore.MetaStoreClientFactory;

public class MetaStoreClientFactoryManagerTest {

  @Test
  public void factoryForThrift() {
    List<MetaStoreClientFactory> list = Collections
        .<MetaStoreClientFactory> singletonList(new ThriftMetaStoreClientFactory());
    MetaStoreClientFactoryManager factoryManager = new MetaStoreClientFactoryManager(list);
    MetaStoreClientFactory clientFactory = factoryManager.factoryForUrl(ThriftMetaStoreClientFactory.ACCEPT_PREFIX);
    assertTrue(clientFactory instanceof ThriftMetaStoreClientFactory);
  }

  @Test(expected = CircusTrainException.class)
  public void factoryForUnsupportedUrl() {
    List<MetaStoreClientFactory> list = Collections.emptyList();
    MetaStoreClientFactoryManager factoryManager = new MetaStoreClientFactoryManager(list);
    factoryManager.factoryForUrl("unsupported:///bla");
  }

}
