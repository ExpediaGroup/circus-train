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

import static com.hotels.bdp.circustrain.core.metastore.TunnelConnectionManagerFactory.FIRST_AVAILABLE_PORT;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.pastdev.jsch.tunnel.TunnelConnectionManager;

import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;
import com.hotels.bdp.circustrain.api.metastore.MetaStoreClientException;
import com.hotels.bdp.circustrain.api.metastore.MetaStoreClientFactory;

public class TunnellingMetaStoreClientSupplier implements Supplier<CloseableMetaStoreClient> {

  private static final Logger LOG = LoggerFactory.getLogger(TunnellingMetaStoreClientSupplier.class);
  public static final String TUNNEL_SSH_LOCAL_HOST = "com.hotels.bdp.circustrain.core.metastore.TunnellingMetaStoreClientFactory.local_host";
  public static final String TUNNEL_SSH_ROUTE = "com.hotels.bdp.circustrain.core.metastore.TunnellingMetaStoreClientFactory.ssh_hops";

  private final String localHost;
  private final String remoteHost;
  private final int remotePort;
  private final String sshRoute;
  private final HiveConf hiveConf;
  private final String name;
  private final TunnelConnectionManagerFactory tunnelConnectionManagerFactory;
  private final MetaStoreClientFactory metaStoreClientFactory;

  /** This does not allow handle multiple URIs in {@link ConfVars.METASTOREURIS}. */
  public TunnellingMetaStoreClientSupplier(
      HiveConf hiveConf,
      String name,
      MetaStoreClientFactory metaStoreClientFactory,
      TunnelConnectionManagerFactory tunnelConnectionManagerFactory) {
    this.hiveConf = hiveConf;
    this.name = name;
    this.tunnelConnectionManagerFactory = tunnelConnectionManagerFactory;

    URI metaStoreUri;
    try {
      metaStoreUri = new URI(hiveConf.getVar(ConfVars.METASTOREURIS));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    remoteHost = metaStoreUri.getHost();
    remotePort = metaStoreUri.getPort();
    sshRoute = hiveConf.get(TUNNEL_SSH_ROUTE);
    localHost = hiveConf.get(TUNNEL_SSH_LOCAL_HOST, "localhost");
    this.metaStoreClientFactory = metaStoreClientFactory;
  }

  @Override
  public CloseableMetaStoreClient get() {
    LOG.debug("Creating tunnel: {}:? -> {} -> {}:{}", localHost, sshRoute, remoteHost, remotePort);
    try {
      TunnelConnectionManager tunnelConnectionManager = tunnelConnectionManagerFactory.create(sshRoute, localHost,
          FIRST_AVAILABLE_PORT, remoteHost, remotePort);
      int localPort = tunnelConnectionManager.getTunnel(remoteHost, remotePort).getAssignedLocalPort();
      tunnelConnectionManager.open();
      LOG.debug("Tunnel created: {}:{} -> {} -> {}:{}", localHost, localPort, sshRoute, remoteHost, remotePort);

      localPort = tunnelConnectionManager.getTunnel(remoteHost, remotePort).getAssignedLocalPort();
      HiveConf localHiveConf = new HiveConf(hiveConf);
      String proxyMetaStoreUris = "thrift://" + localHost + ":" + localPort;
      localHiveConf.setVar(ConfVars.METASTOREURIS, proxyMetaStoreUris);
      LOG.info("Metastore URI {} is being proxied to {}", hiveConf.getVar(ConfVars.METASTOREURIS), proxyMetaStoreUris);
      return new TunnellingMetaStoreClient(metaStoreClientFactory.newInstance(localHiveConf, name),
          tunnelConnectionManager);
    } catch (Exception e) {
      String message = String.format("Unable to establish SSH tunnel: '%s:?' -> '%s' -> '%s:%s'", localHost, sshRoute,
          remoteHost, remotePort);
      throw new MetaStoreClientException(message, e);
    }
  }

}
