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
package com.hotels.bdp.circustrain.api.metastore;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.metastore.compatibility.HiveMetaStoreClientCompatibility;

@RunWith(MockitoJUnitRunner.class)
public class CloseableMetaStoreClientFactoryTest {

  private @Mock IMetaStoreClient delegate;
  private @Mock HiveMetaStoreClientCompatibility compatibility;

  @Test
  public void typical() throws TException {
    try (CloseableMetaStoreClient wrapped = CloseableMetaStoreClientFactory.newInstance(delegate)) {
      wrapped.unlock(1L);
    }
    verify(delegate).unlock(1L);
    verify(delegate).close();
  }

  @Test
  public void compatibility() throws TException {
    when(delegate.getTable("db", "tbl")).thenThrow(new TApplicationException());
    try (CloseableMetaStoreClient wrapped = CloseableMetaStoreClientFactory.newInstance(delegate, compatibility)) {
      wrapped.getTable("db", "tbl");
    }
    verify(compatibility).getTable("db", "tbl");
  }

}
