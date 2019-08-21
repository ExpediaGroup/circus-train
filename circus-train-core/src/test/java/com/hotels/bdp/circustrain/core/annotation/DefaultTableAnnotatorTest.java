package com.hotels.bdp.circustrain.core.annotation;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
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
public class DefaultTableAnnotatorTest {

  private @Mock Supplier<CloseableMetaStoreClient> closeableMetaStoreClientSupplier;
  private @Mock CloseableMetaStoreClient client;
  private @Mock Table table;
  private @Captor ArgumentCaptor<Table> tableArgumentCaptor;
  private DefaultHiveTableAnnotator tableAnnotationService;

  @Before
  public void init() throws TException {
    when(closeableMetaStoreClientSupplier.get()).thenReturn(client);
    when(client.getTable(anyString(), anyString())).thenReturn(table);
    when(table.getParameters()).thenReturn(new HashMap<String, String>());
    tableAnnotationService = new DefaultHiveTableAnnotator(closeableMetaStoreClientSupplier);
  }

  @Test
  public void typical() throws TException {
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    tableAnnotationService.annotateTable("database", "table", properties);
    verify(client).alter_table(anyString(), anyString(), tableArgumentCaptor.capture());
    Map<String, String> parameters = tableArgumentCaptor.getValue()
      .getParameters();
    assertThat(parameters.get("key"), is("value"));
  }

  @Test(expected = CircusTrainException.class)
  public void exception() throws TException {
    doThrow(TException.class).when(client).getTable(anyString(), anyString());
    tableAnnotationService.annotateTable("database", "table", new HashMap<String, String>());
  }

}
