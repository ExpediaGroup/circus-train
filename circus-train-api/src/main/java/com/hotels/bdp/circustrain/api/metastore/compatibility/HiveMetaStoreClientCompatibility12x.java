/**
 * Copyright (C) 2016-2018 Expedia Inc and the original Apache hive-metastore contributors.
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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;

public class HiveMetaStoreClientCompatibility12x implements HiveMetaStoreClientCompatibility {

  private final TServiceClient tServiceClient;

  public HiveMetaStoreClientCompatibility12x(IMetaStoreClient client) {
    while (Proxy.isProxyClass(client.getClass())) {
      InvocationHandler handler = Proxy.getInvocationHandler(client);
      if (handler.getClass().isAssignableFrom(RetryingMetaStoreClient.class)) {
        client = getField(handler, "base");
        continue;
      }
      // Other handlers can be added here
      throw new RuntimeException("Unknown InvocationHandler " + handler.getClass());
    }
    tServiceClient = getField(client, "client");
  }

  private static <T> T getField(Object object, String fieldName) {
    try {
      Field field = object.getClass().getDeclaredField(fieldName);
      T result = null;
      if (field.isAccessible()) {
        result = (T) field.get(object);
      } else {
        field.setAccessible(true);
        result = (T) field.get(object);
        field.setAccessible(false);
      }
      return result;
    } catch (Exception e) {
      throw new RuntimeException("Unable to hack client", e);
    }
  }

  private static Table deepCopy(Table table) {
    return table.deepCopy();
  }

  private void sendBase(String methodName, TBase<?, ?> args) throws TException {
    try {
      Method sendBase = TServiceClient.class.getDeclaredMethod("sendBase", String.class, TBase.class);
      sendBase.setAccessible(true);
      sendBase.invoke(tServiceClient, methodName, args);
      sendBase.setAccessible(false);
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      throw new RuntimeException("Unable to hack sendBase", e);
    }
  }

  private void receiveBase(TBase<?, ?> result, String methodName) throws TException {
    try {
      Method receiveBase = TServiceClient.class.getDeclaredMethod("receiveBase", TBase.class, String.class);
      receiveBase.setAccessible(true);
      receiveBase.invoke(tServiceClient, result, methodName);
      receiveBase.setAccessible(false);
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      throw new RuntimeException("Unable to hack receiveBase", e);
    }
  }

  /*
   * Based on https://github.com/apache/hive/blob/release-1.2.1/metastore/src/java/org/apache/hadoop/hive/metastore/
   * HiveMetaStoreClient.java#L1206
   */
  @Override
  public Table getTable(String dbname, String name) throws MetaException, TException, NoSuchObjectException {
    return deepCopy(get_table(dbname, name));
  }

  /*
   * Copied from Hive 1.2.1 ThriftHiveMetastore.Client#get_table(String,String) - see
   * https://raw.githubusercontent.com/apache/hive/release-1.2.1/metastore/src/gen/thrift/gen-javabean/org/apache/hadoop
   * /hive/metastore/api/ThriftHiveMetastore.java
   */
  private Table get_table(String dbname, String tbl_name) throws MetaException, NoSuchObjectException, TException {
    send_get_table(dbname, tbl_name);
    return recv_get_table();
  }

  /*
   * Copied from Hive 1.2.1 ThriftHiveMetastore.Client#send_get_table(String,String) - see
   * https://raw.githubusercontent.com/apache/hive/release-1.2.1/metastore/src/gen/thrift/gen-javabean/org/apache/hadoop
   * /hive/metastore/api/ThriftHiveMetastore.java
   */
  private void send_get_table(String dbname, String tbl_name) throws TException {
    ThriftHiveMetastore.get_table_args args = new ThriftHiveMetastore.get_table_args();
    args.setDbname(dbname);
    args.setTbl_name(tbl_name);
    sendBase("get_table", args);
  }

  /*
   * Based on Hive 1.2.1 ThriftHiveMetastore.Client#recv_get_table() - see
   * https://raw.githubusercontent.com/apache/hive/release-1.2.1/metastore/src/gen/thrift/gen-javabean/org/apache/hadoop
   * /hive/metastore/api/ThriftHiveMetastore.java
   */
  private Table recv_get_table() throws MetaException, NoSuchObjectException, TException {
    ThriftHiveMetastore.get_table_result result = new ThriftHiveMetastore.get_table_result();
    receiveBase(result, "get_table");
    if (result.isSetSuccess()) {
      return result.getSuccess();
    }
    if (result.getO1() != null) {
      throw result.getO1();
    }
    if (result.getO2() != null) {
      throw result.getO2();
    }
    throw new TApplicationException(5, "get_table failed: unknown result");
  }

}
