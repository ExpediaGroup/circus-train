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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.circustrain.core.metastore.CircusTrainHiveConfVars.SSH_LOCALHOST;
import static com.hotels.bdp.circustrain.core.metastore.CircusTrainHiveConfVars.SSH_ROUTE;

import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.jcraft.jsch.JSchException;

import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;
import com.hotels.bdp.circustrain.api.metastore.MetaStoreClientException;
import com.hotels.hcommon.ssh.MethodChecker;
import com.hotels.hcommon.ssh.TunnelableFactory;
import com.hotels.hcommon.ssh.TunnelableSupplier;

@RunWith(MockitoJUnitRunner.class)
public class TunnellingMetaStoreClientSupplierTest {

  private static final String LOCAL_HOST = "127.0.0.2";
  private static final int LOCAL_PORT = 12345;
  private static final int REMOTE_PORT = 9083;
  private static final String REMOTE_HOST = "emrmaster";
  private static final String ROUTE = "hadoop@ec2";

  private @Mock CloseableMetaStoreClient tunnelable;
  private @Mock TunnelableFactory<CloseableMetaStoreClient> tunnelableFactory;
  private @Mock ThriftMetaStoreClientFactory metaStoreClientFactory;
  private @Mock CloseableMetaStoreClient metaStoreClient;
  private @Captor ArgumentCaptor<HiveConf> localHiveConfCaptor;

  private final HiveConf hiveConf = new HiveConf();
  private TunnellingMetaStoreClientSupplier factory;

  @Before
  public void configureHive() {
    hiveConf.setVar(ConfVars.METASTOREURIS, "thrift://" + REMOTE_HOST + ":" + REMOTE_PORT);
    hiveConf.set(SSH_LOCALHOST.varname, LOCAL_HOST);
    hiveConf.set(SSH_ROUTE.varname, ROUTE);
  }

  @Before
  public void injectMocks() throws Exception {
    when(tunnelableFactory.wrap(any(TunnelableSupplier.class), any(MethodChecker.class), eq(LOCAL_HOST), eq(LOCAL_PORT),
        eq(REMOTE_HOST), eq(REMOTE_PORT))).thenReturn(tunnelable);
    when(metaStoreClientFactory.newInstance(localHiveConfCaptor.capture(), anyString())).thenReturn(metaStoreClient);
  }

  @Test
  public void newInstance() throws Exception {
    factory = new TunnellingMetaStoreClientSupplier(hiveConf, "emr-metastore", metaStoreClientFactory,
        tunnelableFactory);

    factory.get();

    HiveConf localHiveConf = localHiveConfCaptor.getValue();
    assertThat(localHiveConf.getVar(ConfVars.METASTOREURIS), is("thrift://" + LOCAL_HOST + ":" + LOCAL_PORT));
  }

  @Test(expected = RuntimeException.class)
  public void invalidURI() {
    hiveConf.setVar(ConfVars.METASTOREURIS, "#invalid://#host:port");

    new TunnellingMetaStoreClientSupplier(hiveConf, "emr-metastore", metaStoreClientFactory, tunnelableFactory);
  }

  @Test(expected = MetaStoreClientException.class)
  public void metaException() throws MetaException, IOException {
    doThrow(MetaStoreClientException.class).when(metaStoreClientFactory).newInstance(any(HiveConf.class), anyString());

    factory = new TunnellingMetaStoreClientSupplier(hiveConf, "emr-metastore", metaStoreClientFactory,
        tunnelableFactory);

    factory.get();
  }

  @Test(expected = MetaStoreClientException.class)
  public void jschException() throws JSchException {
    reset(tunnelableFactory, metaStoreClientFactory);
    doThrow(JSchException.class).when(tunnelableFactory).wrap(any(TunnelableSupplier.class), any(MethodChecker.class),
        eq(LOCAL_HOST), eq(LOCAL_PORT), eq(REMOTE_HOST), eq(REMOTE_PORT));

    factory = new TunnellingMetaStoreClientSupplier(hiveConf, "emr-metastore", metaStoreClientFactory,
        tunnelableFactory);

    factory.get();
  }

}
