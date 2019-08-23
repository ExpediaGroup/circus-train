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
package com.hotels.bdp.circustrain.core.annotation;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@RunWith(MockitoJUnitRunner.class)
public class HiveTableAnnotatorTest {

  private @Mock Supplier<CloseableMetaStoreClient> closeableMetaStoreClientSupplier;
  private @Mock CloseableMetaStoreClient client;
  private @Mock Table table;
  private @Captor ArgumentCaptor<Table> tableArgumentCaptor;
  private HiveTableAnnotator hiveTableAnnotator;

  @Before
  public void init() throws TException {
    when(closeableMetaStoreClientSupplier.get()).thenReturn(client);
    when(client.getTable(anyString(), anyString())).thenReturn(table);
    when(table.getParameters()).thenReturn(new HashMap<String, String>());
    hiveTableAnnotator = new HiveTableAnnotator(closeableMetaStoreClientSupplier);
  }

  @Test
  public void typical() throws TException {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("key", "value");
    hiveTableAnnotator.annotateTable("database", "table", parameters);
    verify(client).alter_table(anyString(), anyString(), tableArgumentCaptor.capture());
    Map<String, String> tableParameters = tableArgumentCaptor.getValue()
      .getParameters();
    assertThat(tableParameters.get("key"), is("value"));
  }

  @Test
  public void typicalEmptyParameters() throws TException {
    Map<String, String> parameters = new HashMap<>();
    hiveTableAnnotator.annotateTable("database", "table", parameters);
    verify(client, never()).getTable(anyString(), anyString());
    verify(client, never()).alter_table(anyString(), anyString(), any(Table.class));
  }

  @Test(expected = CircusTrainException.class)
  public void exception() throws TException {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("key", "value");
    doThrow(TException.class).when(client).getTable(anyString(), anyString());
    hiveTableAnnotator.annotateTable("database", "table", parameters);
  }

}
