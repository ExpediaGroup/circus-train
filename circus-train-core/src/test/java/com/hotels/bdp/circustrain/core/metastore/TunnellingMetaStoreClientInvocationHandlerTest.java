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
package com.hotels.bdp.circustrain.core.metastore;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.pastdev.jsch.tunnel.TunnelConnectionManager;

import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;

@RunWith(MockitoJUnitRunner.class)
public class TunnellingMetaStoreClientInvocationHandlerTest {

  private @Mock CloseableMetaStoreClient delegate;
  private @Mock TunnelConnectionManager tunnelConnectionManager;

  private TunnellingMetaStoreClientInvocationHandler handler;

  @Before
  public void before() {
    handler = new TunnellingMetaStoreClientInvocationHandler(delegate, tunnelConnectionManager);
  }

  @Test
  public void close() throws Throwable {
    handler.invoke(null, CloseableMetaStoreClient.class.getMethod("close"), null);

    verify(delegate).close();
    verify(tunnelConnectionManager).close();
  }

  @Test
  public void exceptionOnClose() throws Throwable {
    doThrow(IOException.class).when(delegate).close();

    try {
      handler.invoke(null, CloseableMetaStoreClient.class.getMethod("close"), null);
      fail();
    } catch (Exception e) {
      verify(tunnelConnectionManager).close();
    }
  }

  @Test
  public void reconnect() throws Throwable {
    handler.invoke(null, CloseableMetaStoreClient.class.getMethod("reconnect"), null);

    verify(delegate).reconnect();
    verify(tunnelConnectionManager).ensureOpen();
  }

  @Test
  public void decoratedMethods() throws Exception {
    for (Method delegateMethod : CloseableMetaStoreClient.class.getDeclaredMethods()) {
      Method decoratorMethod = TunnellingMetaStoreClientInvocationHandler.class
          .getDeclaredMethod(delegateMethod.getName(), delegateMethod.getParameterTypes());
      Object[] arguments = new Object[delegateMethod.getParameterTypes().length];
      for (int i = 0; i < delegateMethod.getParameterTypes().length; i++) {
        arguments[i] = any(delegateMethod.getParameterTypes()[i]);
      }
      decoratorMethod.invoke(handler, arguments);
      delegateMethod.invoke(verify(delegate), arguments);
    }
  }

  @Test(expected = MetaException.class)
  public void methodThrowsUnderlyingTargetException() throws Throwable {
    Throwable targetException = new MetaException();
    when(delegate.getAllDatabases()).thenThrow(targetException);
    Method method = CloseableMetaStoreClient.class.getMethod("getAllDatabases");
    handler.invoke(null, method, null);
  }

}
