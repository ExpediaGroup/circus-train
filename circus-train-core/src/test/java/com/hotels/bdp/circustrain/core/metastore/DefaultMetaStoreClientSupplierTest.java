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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;
import com.hotels.bdp.circustrain.api.metastore.MetaStoreClientException;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMetaStoreClientSupplierTest {

  @Mock
  private ThriftMetaStoreClientFactory metaStoreClientFactory;
  @Mock
  private CloseableMetaStoreClient metaStoreClient;

  private final HiveConf conf = new HiveConf();
  private final String name = "name";

  private Supplier<CloseableMetaStoreClient> supplier;

  @Before
  public void before() {
    supplier = new DefaultMetaStoreClientSupplier(conf, name, metaStoreClientFactory);
  }

  @Test
  public void typical() throws TException {
    when(metaStoreClientFactory.newInstance(any(HiveConf.class), anyString())).thenReturn(metaStoreClient);

    CloseableMetaStoreClient client = supplier.get();

    assertThat(client, is(metaStoreClient));
  }

  @Test(expected = MetaStoreClientException.class)
  public void exception() {
    doThrow(MetaStoreClientException.class).when(metaStoreClientFactory).newInstance(any(HiveConf.class), anyString());

    supplier.get();
  }

}
