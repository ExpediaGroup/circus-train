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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.thrift.TApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.circustrain.api.metastore.compatibility.HiveMetaStoreClientCompatibility;
import com.hotels.bdp.circustrain.api.metastore.compatibility.HiveMetaStoreClientCompatibility12x;

public final class CloseableMetaStoreClientFactory {
  private static final Logger log = LoggerFactory.getLogger(CloseableMetaStoreClientFactory.class);

  private CloseableMetaStoreClientFactory() {}

  public static CloseableMetaStoreClient newInstance(IMetaStoreClient delegate) {
    HiveMetaStoreClientCompatibility compatibility = null;
    try {
      compatibility = new HiveMetaStoreClientCompatibility12x(delegate);
    } catch (Throwable t) {
      log.warn("Unable to initialize compatibility", t);
    }
    return newInstance(delegate, compatibility);
  }

  @VisibleForTesting
  static CloseableMetaStoreClient newInstance(
      IMetaStoreClient delegate,
      HiveMetaStoreClientCompatibility compatibility) {
    ClassLoader classLoader = CloseableMetaStoreClient.class.getClassLoader();
    Class<?>[] interfaces = new Class<?>[] { CloseableMetaStoreClient.class };
    CloseableMetaStoreClientInvocationHandler handler = new CloseableMetaStoreClientInvocationHandler(delegate,
        compatibility);
    return (CloseableMetaStoreClient) Proxy.newProxyInstance(classLoader, interfaces, handler);
  }

  static class CloseableMetaStoreClientInvocationHandler implements InvocationHandler {

    private final IMetaStoreClient delegate;
    private final HiveMetaStoreClientCompatibility compatibility;

    CloseableMetaStoreClientInvocationHandler(
        IMetaStoreClient delegate,
        HiveMetaStoreClientCompatibility compatibility) {
      this.delegate = delegate;
      this.compatibility = compatibility;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      try {
        return method.invoke(delegate, args);
      } catch (InvocationTargetException e) {
        try {
          if (compatibility != null && e.getCause().getClass().isAssignableFrom(TApplicationException.class)) {
            return invokeCompatibility(method, args);
          }
        } catch (Throwable t) {
          log.warn("Unable to run compatibility for metastore client method {}. Will rethrow original exception: ",
              method.getName(), t);
        }
        throw e.getCause();
      }
    }

    private Object invokeCompatibility(Method method, Object[] args) throws Throwable {
      Class<?>[] argTypes = getTypes(args);
      Method compatibilityMethod = compatibility.getClass().getMethod(method.getName(), argTypes);
      return compatibilityMethod.invoke(compatibility, args);
    }

    private static Class<?>[] getTypes(Object[] args) {
      Class<?>[] argTypes = new Class<?>[args.length];
      for (int i = 0; i < args.length; ++i) {
        argTypes[i] = args[i].getClass();
      }
      return argTypes;
    }

  }

}
