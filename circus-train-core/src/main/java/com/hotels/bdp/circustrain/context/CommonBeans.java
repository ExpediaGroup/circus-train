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

import java.util.ArrayList;
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
import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;
import com.hotels.bdp.circustrain.api.metastore.MetaStoreClientFactory;
import com.hotels.bdp.circustrain.core.conf.MetastoreTunnel;
import com.hotels.bdp.circustrain.core.conf.ReplicaCatalog;
import com.hotels.bdp.circustrain.core.conf.Security;
import com.hotels.bdp.circustrain.core.conf.SourceCatalog;
import com.hotels.bdp.circustrain.core.conf.TunnelMetastoreCatalog;
import com.hotels.bdp.circustrain.core.metastore.CircusTrainHiveConfVars;
import com.hotels.bdp.circustrain.core.metastore.DefaultMetaStoreClientSupplier;
import com.hotels.bdp.circustrain.core.metastore.HiveConfFactory;
import com.hotels.bdp.circustrain.core.metastore.MetaStoreClientFactoryManager;
import com.hotels.bdp.circustrain.core.metastore.ThriftMetaStoreClientFactory;
import com.hotels.bdp.circustrain.core.metastore.TunnellingMetaStoreClientSupplier;

@Order(Ordered.HIGHEST_PRECEDENCE)
@org.springframework.context.annotation.Configuration
public class CommonBeans {
  private static final Logger LOG = LoggerFactory.getLogger(CommonBeans.class);
  public static final String BEAN_BASE_CONF = "baseConf";
  public static final String BEAN_SUPPORTED_SCHEMES = "supportedSchemes";

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

  @Bean(name = BEAN_SUPPORTED_SCHEMES)
  Iterable<String> supportedSchemes() {
    List<String> schemes = new ArrayList<>();
    String alwaysSupported = "hdfs";
    schemes.add(alwaysSupported);
    return schemes;
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
    configureMetastoreTunnel(hiveCatalog.getMetastoreTunnel(), properties);
    putConfigurationProperties(hiveCatalog.getConfigurationProperties(), properties);
    HiveConf hiveConf = new HiveConfFactory(siteXml, properties).newInstance();
    return hiveConf;
  }

  private void configureMetastoreTunnel(MetastoreTunnel metastoreTunnel, Map<String, String> properties) {
    if (metastoreTunnel != null) {
      properties.put(CircusTrainHiveConfVars.SSH_ROUTE.varname, metastoreTunnel.getRoute());
      properties.put(CircusTrainHiveConfVars.SSH_PORT.varname, String.valueOf(metastoreTunnel.getPort()));
      properties.put(CircusTrainHiveConfVars.SSH_LOCALHOST.varname, metastoreTunnel.getLocalhost());
      properties.put(CircusTrainHiveConfVars.SSH_PRIVATE_KEYS.varname, metastoreTunnel.getPrivateKeys());
      properties.put(CircusTrainHiveConfVars.SSH_KNOWN_HOSTS.varname, metastoreTunnel.getKnownHosts());
      properties.put(CircusTrainHiveConfVars.SSH_SESSION_TIMEOUT.varname, String.valueOf(metastoreTunnel.getTimeout()));
      properties.put(CircusTrainHiveConfVars.SSH_STRICT_HOST_KEY_CHECKING.varname,
          metastoreTunnel.getStrictHostKeyChecking());
    }
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

  @Bean
  MetaStoreClientFactoryManager metaStoreClientFactoryManager(List<MetaStoreClientFactory> metaStoreClientFactories) {
    return new MetaStoreClientFactoryManager(metaStoreClientFactories);
  }

  @Bean
  MetaStoreClientFactory thriftMetaStoreClientFactory() {
    return new ThriftMetaStoreClientFactory();
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
      metaStoreUris = ThriftMetaStoreClientFactory.ACCEPT_PREFIX;
    }
    MetaStoreClientFactory sourceMetaStoreClientFactory = metaStoreClientFactoryManager.factoryForUrl(metaStoreUris);
    return metaStoreClientSupplier(sourceCatalog.getName(), sourceHiveConf, sourceCatalog.getMetastoreTunnel(),
        sourceMetaStoreClientFactory);
  }

  @Profile({ Modules.REPLICATION })
  @Bean
  Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier(
      ReplicaCatalog replicaCatalog,
      @Value("#{replicaHiveConf}") HiveConf replicaHiveConf,
      MetaStoreClientFactoryManager metaStoreClientFactoryManager) {
    MetaStoreClientFactory replicaMetaStoreClientFactory = metaStoreClientFactoryManager
        .factoryForUrl(replicaCatalog.getHiveMetastoreUris());
    return metaStoreClientSupplier(replicaCatalog.getName(), replicaHiveConf, replicaCatalog.getMetastoreTunnel(),
        replicaMetaStoreClientFactory);
  }

  private Supplier<CloseableMetaStoreClient> metaStoreClientSupplier(
      String name,
      HiveConf replicaHiveConf,
      MetastoreTunnel metastoreTunnel,
      MetaStoreClientFactory metaStoreClientFactory) {
    if (metastoreTunnel != null) {
      return new TunnellingMetaStoreClientSupplier(replicaHiveConf, name, metaStoreClientFactory);
    } else {
      return new DefaultMetaStoreClientSupplier(replicaHiveConf, name, metaStoreClientFactory);
    }
  }
}
