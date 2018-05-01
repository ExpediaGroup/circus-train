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

import static com.hotels.bdp.circustrain.core.metastore.CircusTrainHiveConfVars.SSH_KNOWN_HOSTS;
import static com.hotels.bdp.circustrain.core.metastore.CircusTrainHiveConfVars.SSH_LOCALHOST;
import static com.hotels.bdp.circustrain.core.metastore.CircusTrainHiveConfVars.SSH_PORT;
import static com.hotels.bdp.circustrain.core.metastore.CircusTrainHiveConfVars.SSH_PRIVATE_KEYS;
import static com.hotels.bdp.circustrain.core.metastore.CircusTrainHiveConfVars.SSH_ROUTE;
import static com.hotels.bdp.circustrain.core.metastore.CircusTrainHiveConfVars.SSH_SESSION_TIMEOUT;
import static com.hotels.bdp.circustrain.core.metastore.CircusTrainHiveConfVars.SSH_STRICT_HOST_KEY_CHECKING;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;
import com.hotels.bdp.circustrain.api.metastore.MetaStoreClientException;
import com.hotels.bdp.circustrain.api.metastore.MetaStoreClientFactory;
import com.hotels.hcommon.ssh.MethodChecker;
import com.hotels.hcommon.ssh.SshException;
import com.hotels.hcommon.ssh.SshSettings;
import com.hotels.hcommon.ssh.TunnelableFactory;
import com.hotels.hcommon.ssh.TunnelableSupplier;

public class TunnellingMetaStoreClientSupplier implements Supplier<CloseableMetaStoreClient> {

  private static String stringValue(HiveConf hiveConf, CircusTrainHiveConfVars var) {
    return hiveConf.get(var.varname);
  }

  private static int intValue(HiveConf hiveConf, CircusTrainHiveConfVars var) {
    return hiveConf.getInt(var.varname, Integer.valueOf(var.defaultValue));
  }

  private static boolean booleanValue(HiveConf hiveConf, CircusTrainHiveConfVars var) {
    String value = stringValue(hiveConf, var).trim().toLowerCase();
    return "true".equals(value) || "yes".equals(value) ? true : false;
  }

  private static int getLocalPort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException | RuntimeException e) {
      throw new SshException("Unable to bind to a free localhost port", e);
    }
  }

  @VisibleForTesting
  static class HiveMetaStoreClientSupplier implements TunnelableSupplier<CloseableMetaStoreClient> {
    private final MetaStoreClientFactory metaStoreClientFactory;
    private final String name;
    final @VisibleForTesting HiveConf hiveConf;

    private HiveMetaStoreClientSupplier(MetaStoreClientFactory metaStoreClientFactory, HiveConf hiveConf, String name) {
      this.metaStoreClientFactory = metaStoreClientFactory;
      this.name = name;
      this.hiveConf = hiveConf;
    }

    @Override
    public CloseableMetaStoreClient get() {
      return metaStoreClientFactory.newInstance(hiveConf, name);
    }

  }

  private final String localHost;
  private final String remoteHost;
  private final int remotePort;
  private final HiveConf hiveConf;
  private final String name;
  private final TunnelableFactory<CloseableMetaStoreClient> tunnelableFactory;
  private final MetaStoreClientFactory metaStoreClientFactory;

  /** This does not allow handle multiple URIs in {@link ConfVars.METASTOREURIS}. */
  public TunnellingMetaStoreClientSupplier(
      HiveConf hiveConf,
      String name,
      MetaStoreClientFactory metaStoreClientFactory) {
    this(hiveConf, name, metaStoreClientFactory,
        new TunnelableFactory<CloseableMetaStoreClient>(SshSettings
            .builder()
            .withRoute(stringValue(hiveConf, SSH_ROUTE))
            .withSshPort(intValue(hiveConf, SSH_PORT))
            .withPrivateKeys(stringValue(hiveConf, SSH_PRIVATE_KEYS))
            .withKnownHosts(stringValue(hiveConf, SSH_KNOWN_HOSTS))
            .withSessionTimeout(intValue(hiveConf, SSH_SESSION_TIMEOUT))
            .withStrictHostKeyChecking(booleanValue(hiveConf, SSH_STRICT_HOST_KEY_CHECKING))
            .build()));
  }

  @VisibleForTesting
  TunnellingMetaStoreClientSupplier(
      HiveConf hiveConf,
      String name,
      MetaStoreClientFactory metaStoreClientFactory,
      TunnelableFactory<CloseableMetaStoreClient> tunnelableFactory) {
    this.hiveConf = hiveConf;
    this.name = name;
    this.tunnelableFactory = tunnelableFactory;

    URI metaStoreUri;
    try {
      metaStoreUri = new URI(hiveConf.getVar(ConfVars.METASTOREURIS));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    remoteHost = metaStoreUri.getHost();
    remotePort = metaStoreUri.getPort();
    localHost = hiveConf.get(SSH_LOCALHOST.varname, "localhost");
    this.metaStoreClientFactory = metaStoreClientFactory;
  }

  @Override
  public CloseableMetaStoreClient get() {
    try {
      int localPort = getLocalPort();
      HiveConf localHiveConf = localHiveConf(hiveConf, localHost, localPort);
      HiveMetaStoreClientSupplier supplier = new HiveMetaStoreClientSupplier(metaStoreClientFactory, localHiveConf,
          name);
      return (CloseableMetaStoreClient) tunnelableFactory.wrap(supplier, MethodChecker.DEFAULT, localHost, localPort,
          remoteHost, remotePort);
    } catch (Exception e) {
      throw new MetaStoreClientException("Unable to create tunnelled HiveMetaStoreClient", e);
    }
  }

  private static HiveConf localHiveConf(HiveConf hiveConf, String localHost, int localPort) {
    HiveConf localHiveConf = new HiveConf(hiveConf);
    String proxyMetaStoreUris = "thrift://" + localHost + ":" + localPort;
    localHiveConf.setVar(ConfVars.METASTOREURIS, proxyMetaStoreUris);
    return localHiveConf;
  }

}
