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
package com.hotels.bdp.circustrain.api.metastore.compatibility;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.get_table_args;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;

@RunWith(MockitoJUnitRunner.class)
public class HiveMetaStoreClientCompatibility12xTest {

  private static final String DATABASE = "db";
  private static final String TABLE = "tbl";

  private static class RelaxedTServiceClient extends ThriftHiveMetastore.Client {
    private final ThriftHiveMetastore.Client delegate;

    public RelaxedTServiceClient(ThriftHiveMetastore.Client delegate) {
      super(delegate.getInputProtocol());
      this.delegate = delegate;
    }

    @Override
    public GetTableResult get_table_req(GetTableRequest req) throws MetaException, NoSuchObjectException, TException {
      // Safeguard: this method should not be invoked since we are testing directrly calling the compatibility class
      throw new TApplicationException();
    }

    @Override
    protected void sendBase(String methodName, TBase<?, ?> args) throws TException {
      try {
        Method sendBase = TServiceClient.class.getDeclaredMethod("sendBase", String.class, TBase.class);
        sendBase.setAccessible(true);
        sendBase.invoke(delegate, methodName, args);
      } catch (Exception e) {
        throw new TException(e);
      }
    }

    @Override
    protected void receiveBase(TBase<?, ?> result, String methodName) throws TException {
      try {
        Method sendBase = TServiceClient.class.getDeclaredMethod("receiveBase", TBase.class, String.class);
        sendBase.setAccessible(true);
        sendBase.invoke(delegate, result, methodName);
      } catch (Exception e) {
        throw new TException(e);
      }
    }
  }

  public final @Rule ThriftHiveMetaStoreJUnitRule metastore = new ThriftHiveMetaStoreJUnitRule(DATABASE);

  @Before
  public void init() throws Exception {
    Table table = new Table();
    table.setDbName(DATABASE);
    table.setTableName(TABLE);
    table.setSd(new StorageDescriptor());
    table.getSd().setCols(ImmutableList.of(new FieldSchema("col", "string", null)));
    table.getSd().setSerdeInfo(new SerDeInfo());
    metastore.client().createTable(table);
  }

  private RelaxedTServiceClient prepareClient(IMetaStoreClient metastoreClient) throws Exception {
    Field field = metastoreClient.getClass().getDeclaredField("client");
    field.setAccessible(true);
    ThriftHiveMetastore.Client client = (ThriftHiveMetastore.Client) field.get(metastoreClient);
    RelaxedTServiceClient relaxedTServiceClient = spy(new RelaxedTServiceClient(client));
    field.set(metastoreClient, relaxedTServiceClient);
    return relaxedTServiceClient;
  }

  private RelaxedTServiceClient prepareRetryingClient(IMetaStoreClient metastoreClient) throws Exception {
    InvocationHandler handler = Proxy.getInvocationHandler(metastoreClient);
    Field field = handler.getClass().getDeclaredField("base");
    field.setAccessible(true);
    IMetaStoreClient base = (IMetaStoreClient) field.get(handler);
    return prepareClient(base);
  }

  @Test
  public void properClient() throws Exception {
    IMetaStoreClient metastoreClient = metastore.client();
    RelaxedTServiceClient tServiceClient = prepareClient(metastoreClient);
    HiveMetaStoreClientCompatibility12x compatibility = new HiveMetaStoreClientCompatibility12x(metastoreClient);
    compatibility.getTable(DATABASE, TABLE);
    verify(tServiceClient).sendBase(eq("get_table"), eq(new get_table_args(DATABASE, TABLE)));
  }

  @Test
  public void prepareRetryingClient() throws Exception {
    IMetaStoreClient metastoreClient = RetryingMetaStoreClient.getProxy(metastore.conf(), new HiveMetaHookLoader() {
      @Override
      public HiveMetaHook getHook(Table tbl) throws MetaException {
        return null;
      }
    }, HiveMetaStoreClient.class.getName());
    RelaxedTServiceClient tServiceClient = prepareRetryingClient(metastoreClient);
    HiveMetaStoreClientCompatibility12x compatibility = new HiveMetaStoreClientCompatibility12x(metastoreClient);
    compatibility.getTable(DATABASE, TABLE);
    verify(tServiceClient).sendBase(eq("get_table"), eq(new get_table_args(DATABASE, TABLE)));
  }

}
