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
package com.hotels.bdp.circustrain.api.metastore;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;

public final class CloseableMetaStoreClientFactory {

  private CloseableMetaStoreClientFactory() {}

  public static CloseableMetaStoreClient newInstance(IMetaStoreClient delegate) {
    ClassLoader classLoader = CloseableMetaStoreClient.class.getClassLoader();
    Class<?>[] interfaces = new Class<?>[] { CloseableMetaStoreClient.class };
    CloseableMetaStoreClientInvocationHandler handler = new CloseableMetaStoreClientInvocationHandler(delegate);
    return (CloseableMetaStoreClient) Proxy.newProxyInstance(classLoader, interfaces, handler);
  }

  static class CloseableMetaStoreClientInvocationHandler implements InvocationHandler {

    private final IMetaStoreClient delegate;

    CloseableMetaStoreClientInvocationHandler(IMetaStoreClient delegate) {
      this.delegate = delegate;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      try {
        return method.invoke(delegate, args);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }

  }

}
