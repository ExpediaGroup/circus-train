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

package com.hotels.bdp.circustrain.context;

import static org.apache.hadoop.security.alias.CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.conf.ReplicaCatalog;
import com.hotels.bdp.circustrain.api.conf.Security;
import com.hotels.bdp.circustrain.api.conf.SourceCatalog;
import com.hotels.bdp.circustrain.api.conf.TunnelMetastoreCatalog;
import com.hotels.bdp.circustrain.core.metastore.ConditionalMetaStoreClientFactory;
import com.hotels.bdp.circustrain.core.metastore.MetaStoreClientFactoryManager;
import com.hotels.bdp.circustrain.core.metastore.ThriftHiveMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.supplier.HiveMetaStoreClientSupplier;
import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;
import com.hotels.hcommon.hive.metastore.client.tunnelling.TunnellingMetaStoreClientSupplierBuilder;
import com.hotels.hcommon.hive.metastore.conf.HiveConfFactory;

@Order(Ordered.HIGHEST_PRECEDENCE)
@org.springframework.context.annotation.Configuration
public class CommonBeans {
  private static final Logger LOG = LoggerFactory.getLogger(CommonBeans.class);
  public static final String BEAN_BASE_CONF = "baseConf";

  @Bean(name = BEAN_BASE_CONF)
  Configuration baseConf(Security security) {
    Map<String, String> properties = new HashMap<>();
    setCredentialProviderPath(security, properties);
    Configuration conf = new Configuration();
    for (Entry<String, String> entry : properties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    return conf;
  }

  @Profile({ Modules.REPLICATION })
  @Bean
  HiveConf sourceHiveConf(SourceCatalog sourceCatalog, @Qualifier("baseConf") Configuration baseConf) {
    return newHiveConf(sourceCatalog, baseConf);
  }

  @Bean
  HiveConf replicaHiveConf(ReplicaCatalog replicaCatalog, @Qualifier("baseConf") Configuration baseConf) {
    return newHiveConf(replicaCatalog, baseConf);
  }

  private HiveConf newHiveConf(TunnelMetastoreCatalog hiveCatalog, Configuration baseConf) {
    List<String> siteXml = hiveCatalog.getSiteXml();
    if (CollectionUtils.isEmpty(siteXml)) {
      LOG.info("No Hadoop site XML is defined for catalog {}.", hiveCatalog.getName());
    }
    Map<String, String> properties = new HashMap<>();
    for (Entry<String, String> entry : baseConf) {
      properties.put(entry.getKey(), entry.getValue());
    }
    if (hiveCatalog.getHiveMetastoreUris() != null) {
      properties.put(ConfVars.METASTOREURIS.varname, hiveCatalog.getHiveMetastoreUris());
    }
    putConfigurationProperties(hiveCatalog.getConfigurationProperties(), properties);
    HiveConf hiveConf = new HiveConfFactory(siteXml, properties).newInstance();
    return hiveConf;
  }

  private void setCredentialProviderPath(Security security, Map<String, String> properties) {
    if (security.getCredentialProvider() != null) {
      // TODO perhaps we should have a source catalog scoped credential provider instead on one specific to S3?
      properties.put(CREDENTIAL_PROVIDER_PATH, security.getCredentialProvider());
    }
  }

  private void putConfigurationProperties(Map<String, String> configurationProperties, Map<String, String> properties) {
    if (configurationProperties != null) {
      properties.putAll(configurationProperties);
    }
  }

  @Profile({ Modules.REPLICATION })
  @Bean
  ConditionalMetaStoreClientFactory thriftHiveMetaStoreClientFactory() {
    return new ThriftHiveMetaStoreClientFactory();
  }

  @Profile({ Modules.REPLICATION })
  @Bean
  MetaStoreClientFactoryManager metaStoreClientFactoryManager(List<ConditionalMetaStoreClientFactory> factories) {
    return new MetaStoreClientFactoryManager(factories);
  }

  @Profile({ Modules.REPLICATION })
  @Bean
  Supplier<CloseableMetaStoreClient> sourceMetaStoreClientSupplier(
      SourceCatalog sourceCatalog,
      @Value("#{sourceHiveConf}") HiveConf sourceHiveConf,
      MetaStoreClientFactoryManager metaStoreClientFactoryManager) {
    String metaStoreUris = sourceCatalog.getHiveMetastoreUris();
    if (metaStoreUris == null) {
      // Default to Thrift is not specified - optional attribute in SourceCatalog
      metaStoreUris = ThriftHiveMetaStoreClientFactory.ACCEPT_PREFIX;
    }
    MetaStoreClientFactory sourceMetaStoreClientFactory = metaStoreClientFactoryManager.factoryForUrl(metaStoreUris);
    return metaStoreClientSupplier(sourceHiveConf, sourceCatalog.getName(), sourceCatalog.getMetastoreTunnel(),
        sourceMetaStoreClientFactory);
  }

  @Profile({ Modules.REPLICATION })
  @Bean
  Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier(
      ReplicaCatalog replicaCatalog,
      @Value("#{replicaHiveConf}") HiveConf replicaHiveConf,
      MetaStoreClientFactoryManager metaStoreClientFactoryManager) {
    String metaStoreUris = replicaCatalog.getHiveMetastoreUris();
    if (metaStoreUris == null) {
      // Default to Thrift is not specified - optional attribute in SourceCatalog
      metaStoreUris = ThriftHiveMetaStoreClientFactory.ACCEPT_PREFIX;
    }
    MetaStoreClientFactory replicaMetaStoreClientFactory = metaStoreClientFactoryManager.factoryForUrl(metaStoreUris);
    return metaStoreClientSupplier(replicaHiveConf, replicaCatalog.getName(), replicaCatalog.getMetastoreTunnel(),
        replicaMetaStoreClientFactory);
  }

  private Supplier<CloseableMetaStoreClient> metaStoreClientSupplier(
      HiveConf hiveConf,
      String name,
      MetastoreTunnel metastoreTunnel,
      MetaStoreClientFactory metaStoreClientFactory) {
    if (metastoreTunnel != null) {
      return new TunnellingMetaStoreClientSupplierBuilder()
          .withName(name)
          .withRoute(metastoreTunnel.getRoute())
          .withKnownHosts(metastoreTunnel.getKnownHosts())
          .withLocalHost(metastoreTunnel.getLocalhost())
          .withPort(metastoreTunnel.getPort())
          .withPrivateKeys(metastoreTunnel.getPrivateKeys())
          .withTimeout(metastoreTunnel.getTimeout())
          .withStrictHostKeyChecking(metastoreTunnel.getStrictHostKeyChecking())
          .build(hiveConf, metaStoreClientFactory);
    } else {
      return new HiveMetaStoreClientSupplier(metaStoreClientFactory, hiveConf, name);
    }
  }
}
