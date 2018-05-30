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
package com.hotels.bdp.circustrain.tool.filter;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.Banner.Mode;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.validation.BindException;
import org.springframework.validation.ObjectError;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.core.HiveEndpointFactory;
import com.hotels.bdp.circustrain.core.conf.ReplicaCatalog;
import com.hotels.bdp.circustrain.core.conf.SourceCatalog;
import com.hotels.bdp.circustrain.core.conf.SpringExpressionParser;
import com.hotels.bdp.circustrain.core.conf.TableReplication;
import com.hotels.bdp.circustrain.manifest.ManifestAttributes;
import com.hotels.bdp.circustrain.tool.core.endpoint.ReplicaHiveEndpoint;
import com.hotels.bdp.circustrain.tool.core.endpoint.SourceHiveEndpoint;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@SpringBootApplication
@EnableConfigurationProperties
@ComponentScan({
    "com.hotels.bdp.circustrain.tool.filter",
    "com.hotels.bdp.circustrain.context",
    "com.hotels.bdp.circustrain.core.conf" })
public class FilterTool {
  private static final Logger LOG = LoggerFactory.getLogger(FilterTool.class);

  public static void main(String[] args) throws Exception {
    // below is output *before* logging is configured so will appear on console
    logVersionInfo();

    try {
      SpringApplication.exit(new SpringApplicationBuilder(FilterTool.class)
          .properties("spring.config.location:${config:null}")
          .properties("spring.profiles.active:" + Modules.REPLICATION)
          .properties("instance.home:${user.home}")
          .properties("instance.name:${source-catalog.name}_${replica-catalog.name}")
          .bannerMode(Mode.OFF)
          .registerShutdownHook(true)
          .build()
          .run(args));
    } catch (BeanCreationException e) {
      if (e.getMostSpecificCause() instanceof BindException) {
        printFilterToolHelp(((BindException) e.getMostSpecificCause()).getAllErrors());
      }
      throw e;
    }
  }

  private static void printFilterToolHelp(List<ObjectError> allErrors) {
    System.out.println(new FilterToolHelp(allErrors));
  }

  FilterTool() {
    // below is output *after* logging is configured so will appear in log file
    logVersionInfo();
  }

  private static void logVersionInfo() {
    ManifestAttributes manifestAttributes = new ManifestAttributes(FilterTool.class);
    LOG.info("{}", manifestAttributes);
  }

  @Bean
  FilterGeneratorFactory filterGeneratorFactory(
      HiveEndpointFactory<SourceHiveEndpoint> sourceFactory,
      HiveEndpointFactory<ReplicaHiveEndpoint> replicaFactory,
      SpringExpressionParser expressionParser) {
    return new FilterGeneratorFactory(sourceFactory, replicaFactory, expressionParser);
  }

  @Bean
  HiveEndpointFactory<SourceHiveEndpoint> sourceFactory(
      final SourceCatalog sourceCatalog,
      final HiveConf sourceHiveConf,
      final Supplier<CloseableMetaStoreClient> sourceMetaStoreClientSupplier) {
    return new HiveEndpointFactory<SourceHiveEndpoint>() {
      @Override
      public SourceHiveEndpoint newInstance(TableReplication tableReplication) {
        return new SourceHiveEndpoint(sourceCatalog.getName(), sourceHiveConf, sourceMetaStoreClientSupplier);
      }
    };
  }

  @Bean
  HiveEndpointFactory<ReplicaHiveEndpoint> replicaFactory(
      final ReplicaCatalog replicaCatalog,
      final HiveConf replicaHiveConf,
      final Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier) {
    return new HiveEndpointFactory<ReplicaHiveEndpoint>() {
      @Override
      public ReplicaHiveEndpoint newInstance(TableReplication tableReplication) {
        return new ReplicaHiveEndpoint(replicaCatalog.getName(), replicaHiveConf, replicaMetaStoreClientSupplier);
      }
    };
  }
}
