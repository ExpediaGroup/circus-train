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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.Rule;
import org.junit.Test;

import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;
import com.hotels.bdp.circustrain.api.metastore.MetaStoreClientException;
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;

public class ThriftMetaStoreClientFactoryTest {

  public @Rule ThriftHiveMetaStoreJUnitRule hive = new ThriftHiveMetaStoreJUnitRule();

  private final ThriftMetaStoreClientFactory factory = new ThriftMetaStoreClientFactory();

  @Test
  public void accepts() {
    assertTrue(factory.accepts("thrift:"));
    assertFalse(factory.accepts("something-else:"));
    assertFalse(factory.accepts(""));
    assertFalse(factory.accepts(null));
  }

  @Test
  public void newInstance() throws Exception {
    CloseableMetaStoreClient client = factory.newInstance(hive.conf(), "name");
    assertNotNull(client.getDatabase(hive.databaseName()));
  }

  @Test(expected = MetaStoreClientException.class)
  public void newInstanceCannotConnectThrowsMetaStoreClientException() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setVar(ConfVars.METASTOREURIS, "thrift://ghost:1234");
    factory.newInstance(conf, "name");
  }

}
