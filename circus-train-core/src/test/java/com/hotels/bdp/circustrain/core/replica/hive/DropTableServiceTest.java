/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
package com.hotels.bdp.circustrain.core.replica.hive;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.core.transformation.TableParametersTransformation;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@RunWith(MockitoJUnitRunner.class)
public class DropTableServiceTest {

  private static final String TABLE_NAME = "table";
  private static final String DB_NAME = "db";

  private @Mock CloseableMetaStoreClient client;
  private @Mock TableParametersTransformation tableParametersTransformation;
  private @Captor ArgumentCaptor<Table> tableCaptor;

  private DropTableService service;
  private Table table = new Table();

  @Before
  public void setUp() throws TException {
    service = new DropTableService(tableParametersTransformation);
    table.setTableName(TABLE_NAME);
    table.setDbName(DB_NAME);
    when(client.getTable(DB_NAME, TABLE_NAME)).thenReturn(table);
  }

  @Test
  public void removeParamsAndDropNullParams() throws TException {
    service.removeCustomParamsAndDrop(client, DB_NAME, TABLE_NAME);

    verify(client).dropTable(DB_NAME, TABLE_NAME, false, true);
    verify(client).getTable(DB_NAME, TABLE_NAME);
    verifyNoMoreInteractions(client);
  }

  @Test
  public void removeParamsAndDropEmptyParams() throws TException {
    table.setParameters(Collections.emptyMap());

    service.removeCustomParamsAndDrop(client, DB_NAME, TABLE_NAME);

    verify(client).getTable(DB_NAME, TABLE_NAME);
    verify(client).dropTable(DB_NAME, TABLE_NAME, false, true);
    verifyNoMoreInteractions(client);
  }

  @Test
  public void removeParamsAndDrop() throws TException {
    HashMap<String, String> params = new HashMap<>();
    params.put("key", "value");
    table.setParameters(params);

    service.removeCustomParamsAndDrop(client, DB_NAME, TABLE_NAME);

    verify(client).getTable(DB_NAME, TABLE_NAME);
    verify(client).dropTable(DB_NAME, TABLE_NAME, false, true);
    verifyNoMoreInteractions(client);
  }

  @Test
  public void removeParamsAndDropWithTransformationParams() throws TException {
    HashMap<String, String> transformationParams = new HashMap<>();
    transformationParams.put("transformationKey", "value");
    when(tableParametersTransformation.getTableParameters()).thenReturn(transformationParams);
    HashMap<String, String> params = new HashMap<>();
    params.put("key", "value");
    params.put("transformationKey", "value");
    table.setParameters(params);

    service.removeCustomParamsAndDrop(client, DB_NAME, TABLE_NAME);

    verify(client).getTable(DB_NAME, TABLE_NAME);
    verify(client).alter_table(eq(DB_NAME), eq(TABLE_NAME), tableCaptor.capture());
    verify(client).dropTable(DB_NAME, TABLE_NAME, false, true);
    verifyNoMoreInteractions(client);
    List<Table> capturedTables = tableCaptor.getAllValues();
    assertThat(capturedTables.size(), is(1));
    Map<String, String> parameters = capturedTables.get(0).getParameters();
    assertThat(parameters.size(), is(1));
    assertThat(parameters.get("key"), is("value"));
  }

  @Test
  public void removeParamsAndDropTableDoesNotExist() throws TException {
    doThrow(new NoSuchObjectException()).when(client).getTable(DB_NAME, TABLE_NAME);

    service.removeCustomParamsAndDrop(client, DB_NAME, TABLE_NAME);

    verify(client).getTable(DB_NAME, TABLE_NAME);
    verifyNoMoreInteractions(client);
  }
}
