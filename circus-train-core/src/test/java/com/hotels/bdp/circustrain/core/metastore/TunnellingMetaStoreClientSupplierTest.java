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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.circustrain.core.metastore.TunnellingMetaStoreClientSupplier.TUNNEL_SSH_ROUTE;
import static com.hotels.bdp.circustrain.core.metastore.TunnellingMetaStoreClientSupplier.TUNNEL_SSH_LOCAL_HOST;

import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.jcraft.jsch.JSchException;
import com.pastdev.jsch.tunnel.Tunnel;
import com.pastdev.jsch.tunnel.TunnelConnectionManager;

import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;
import com.hotels.bdp.circustrain.api.metastore.MetaStoreClientException;

@RunWith(MockitoJUnitRunner.class)
public class TunnellingMetaStoreClientSupplierTest {

  private static final String LOCAL_HOST = "127.0.0.2";
  private static final int LOCAL_PORT = 12345;
  private static final int REMOTE_PORT = 9083;
  private static final String REMOTE_HOST = "emrmaster";
  private static final String ROUTE = "hadoop@ec2";

  private @Mock Tunnel tunnel;
  private @Mock TunnelConnectionManager tunnelConnectionManager;
  private @Mock TunnelConnectionManagerFactory tunnelConnectionManagerFactory;
  private @Mock ThriftMetaStoreClientFactory metaStoreClientFactory;
  private @Mock CloseableMetaStoreClient metaStoreClient;
  private @Captor ArgumentCaptor<HiveConf> localHiveConfCaptor;

  private final HiveConf hiveConf = new HiveConf();
  private TunnellingMetaStoreClientSupplier factory;

  @Before
  public void configureHive() {
    hiveConf.setVar(ConfVars.METASTOREURIS, "thrift://" + REMOTE_HOST + ":" + REMOTE_PORT);
    hiveConf.set(TUNNEL_SSH_LOCAL_HOST, LOCAL_HOST);
    hiveConf.set(TUNNEL_SSH_ROUTE, ROUTE);
  }

  @Before
  public void injectMocks() throws Exception {
    when(tunnel.getAssignedLocalPort()).thenReturn(LOCAL_PORT);
    when(tunnelConnectionManager.getTunnel(REMOTE_HOST, REMOTE_PORT)).thenReturn(tunnel);
    when(tunnelConnectionManagerFactory.create(ROUTE, LOCAL_HOST, 0, REMOTE_HOST, REMOTE_PORT))
        .thenReturn(tunnelConnectionManager);
    when(metaStoreClientFactory.newInstance(localHiveConfCaptor.capture(), anyString())).thenReturn(metaStoreClient);
  }

  @Test
  public void newInstance() throws Exception {
    factory = new TunnellingMetaStoreClientSupplier(hiveConf, "emr-metastore", metaStoreClientFactory,
        tunnelConnectionManagerFactory);

    IMetaStoreClient newInstance = factory.get();
    // Verify some delegation
    newInstance.unlock(1L);

    verify(tunnelConnectionManager).open();
    verify(metaStoreClient).unlock(1L);

    HiveConf localHiveConf = localHiveConfCaptor.getValue();
    assertThat(localHiveConf.getVar(ConfVars.METASTOREURIS), is("thrift://" + LOCAL_HOST + ":" + LOCAL_PORT));
  }

  @Test
  public void close() throws Exception {
    factory = new TunnellingMetaStoreClientSupplier(hiveConf, "emr-metastore", metaStoreClientFactory,
        tunnelConnectionManagerFactory);

    IMetaStoreClient newInstance = factory.get();
    newInstance.close();

    InOrder inOrder = inOrder(metaStoreClient, tunnelConnectionManager);
    inOrder.verify(metaStoreClient).close();
    inOrder.verify(tunnelConnectionManager).close();
  }

  @Test(expected = RuntimeException.class)
  public void invalidURI() {
    hiveConf.setVar(ConfVars.METASTOREURIS, "#invalid://#host:port");

    new TunnellingMetaStoreClientSupplier(hiveConf, "emr-metastore", metaStoreClientFactory,
        tunnelConnectionManagerFactory);
  }

  @Test(expected = MetaStoreClientException.class)
  public void metaException() throws MetaException, IOException {
    doThrow(MetaStoreClientException.class).when(metaStoreClientFactory).newInstance(any(HiveConf.class), anyString());

    factory = new TunnellingMetaStoreClientSupplier(hiveConf, "emr-metastore", metaStoreClientFactory,
        tunnelConnectionManagerFactory);

    factory.get();
  }

  @Test(expected = MetaStoreClientException.class)
  public void jschException() throws JSchException {
    doThrow(JSchException.class).when(tunnelConnectionManager).open();

    factory = new TunnellingMetaStoreClientSupplier(hiveConf, "emr-metastore", metaStoreClientFactory,
        tunnelConnectionManagerFactory);

    factory.get();
  }

}
