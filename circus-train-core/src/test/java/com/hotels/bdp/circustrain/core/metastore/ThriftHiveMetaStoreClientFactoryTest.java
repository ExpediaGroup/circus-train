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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ThriftHiveMetaStoreClientFactoryTest {

  @Test
  public void accepts() {
    ThriftHiveMetaStoreClientFactory factory = new ThriftHiveMetaStoreClientFactory();
    assertTrue(factory.accepts("thrift:"));
    assertFalse(factory.accepts("something-else:"));
    assertFalse(factory.accepts(""));
    assertFalse(factory.accepts(null));
  }
}
