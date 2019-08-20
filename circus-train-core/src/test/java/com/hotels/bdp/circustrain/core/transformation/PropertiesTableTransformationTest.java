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
package com.hotels.bdp.circustrain.core.transformation;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Supplier;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@RunWith(MockitoJUnitRunner.class)
public class PropertiesTableTransformationTest {

  private @Mock Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier;
  private @Mock CloseableMetaStoreClient client;
  private @Captor ArgumentCaptor<Table> tableArgumentCaptor;
  private Table table = new Table();

  @Before
  public void init() {
    when(replicaMetaStoreClientSupplier.get()).thenReturn(client);
    table.setTableName("table");
    table.setDbName("database");
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put("key", "value");
    table.setParameters(parameters);
  }

  @Test
  public void transform() throws Exception {
    Map<String, String> additionalProperties = Collections.singletonMap("key2", "value2");
    PropertiesTableTransformation propertiesTableTransformation = new PropertiesTableTransformation(
      replicaMetaStoreClientSupplier, additionalProperties);
    propertiesTableTransformation.transform(table);
    verify(client).alter_table(any(String.class), any(String.class), tableArgumentCaptor.capture());
    Map<String, String> parameters = tableArgumentCaptor.getValue()
      .getParameters();
    assertThat(parameters.get("key"), is("value"));
    assertThat(parameters.get("key2"), is("value2"));
  }
}
