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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.jcraft.jsch.JSchException;
import com.pastdev.jsch.tunnel.TunnelConnectionManager;

import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;

class TunnellingMetaStoreClientInvocationHandler implements InvocationHandler {

  private final CloseableMetaStoreClient delegate;
  private final TunnelConnectionManager tunnelConnectionManager;

  TunnellingMetaStoreClientInvocationHandler(
      CloseableMetaStoreClient delegate,
      TunnelConnectionManager tunnelConnectionManager) {
    this.delegate = delegate;
    this.tunnelConnectionManager = tunnelConnectionManager;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

    switch (method.getName()) {
    case "close":
      try {
        return invoke(method, args);
      } finally {
        tunnelConnectionManager.close();
      }
    case "reconnect":
      try {
        tunnelConnectionManager.ensureOpen();
      } catch (JSchException e) {
        throw new RuntimeException("Unable to reopen tunnel connections", e);
      }
    default:
      return invoke(method, args);
    }

  }

  private Object invoke(Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(delegate, args);
    } catch (InvocationTargetException e) {
      throw e.getTargetException();
    }
  }

}
