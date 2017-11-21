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
package com.hotels.bdp.circustrain.core.metastore;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.pastdev.jsch.tunnel.TunnelConnectionManager;

@RunWith(MockitoJUnitRunner.class)
public class TunnellingMetaStoreClientTest {

  private @Mock IMetaStoreClient delegate;
  private @Mock TunnelConnectionManager tunnelConnectionManager;

  private TunnellingMetaStoreClient client;

  @Before
  public void before() {
    client = new TunnellingMetaStoreClient(delegate, tunnelConnectionManager);
  }

  @Test
  public void close() {
    client.close();

    verify(delegate).close();
    verify(tunnelConnectionManager).close();
  }

  @Test
  public void exceptionOnClose() {
    doThrow(IOException.class).when(delegate).close();

    try {
      client.close();
      fail();
    } catch (Exception e) {
      verify(tunnelConnectionManager).close();
    }
  }

  @Test
  public void decoratedMethods() throws Exception {
    for (Method delegateMethod : IMetaStoreClient.class.getDeclaredMethods()) {
      Method decoratorMethod = TunnellingMetaStoreClient.class.getDeclaredMethod(delegateMethod.getName(),
          delegateMethod.getParameterTypes());
      Object[] arguments = new Object[delegateMethod.getParameterTypes().length];
      for (int i = 0; i < delegateMethod.getParameterTypes().length; i++) {
        arguments[i] = any(delegateMethod.getParameterTypes()[i]);
      }
      decoratorMethod.invoke(client, arguments);
      delegateMethod.invoke(verify(delegate), arguments);
    }
  }

}
