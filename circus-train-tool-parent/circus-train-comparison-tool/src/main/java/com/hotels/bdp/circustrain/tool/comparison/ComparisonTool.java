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
package com.hotels.bdp.circustrain.tool.comparison;

import static com.hotels.bdp.circustrain.comparator.api.ComparatorType.FULL_COMPARISON;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.ApplicationArguments;
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
import com.hotels.bdp.circustrain.comparator.ComparatorRegistry;
import com.hotels.bdp.circustrain.comparator.api.DiffListener;
import com.hotels.bdp.circustrain.conf.ReplicaCatalog;
import com.hotels.bdp.circustrain.conf.SourceCatalog;
import com.hotels.bdp.circustrain.core.HiveEndpoint;
import com.hotels.bdp.circustrain.manifest.ManifestAttributes;
import com.hotels.bdp.circustrain.tool.core.endpoint.ReplicaHiveEndpoint;
import com.hotels.bdp.circustrain.tool.core.endpoint.SourceHiveEndpoint;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@SpringBootApplication
@EnableConfigurationProperties
@ComponentScan({
    "com.hotels.bdp.circustrain.tool.comparison",
    "com.hotels.bdp.circustrain.context",
    "com.hotels.bdp.circustrain.core.conf" })
public class ComparisonTool {
  private static final Logger LOG = LoggerFactory.getLogger(ComparisonTool.class);

  public static void main(String[] args) throws Exception {
    // below is output *before* logging is configured so will appear on console
    logVersionInfo();

    try {
      SpringApplication.exit(new SpringApplicationBuilder(ComparisonTool.class)
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
        printComparisonToolHelp(((BindException) e.getMostSpecificCause()).getAllErrors());
        throw e;
      }
      if (e.getMostSpecificCause() instanceof IllegalArgumentException) {
        LOG.error(e.getMessage(), e);
        printComparisonToolHelp(Collections.<ObjectError> emptyList());
      }
    }
  }

  private static void printComparisonToolHelp(List<ObjectError> allErrors) {
    System.out.println(new ComparisonToolHelp(allErrors));
  }

  ComparisonTool() {
    // below is output *after* logging is configured so will appear in log file
    logVersionInfo();
  }

  private static void logVersionInfo() {
    ManifestAttributes manifestAttributes = new ManifestAttributes(ComparisonTool.class);
    LOG.info("{}", manifestAttributes);
  }

  @Bean
  ComparatorRegistry comparatorRegistry() {
    return new ComparatorRegistry(FULL_COMPARISON);
  }

  @Bean
  HiveEndpoint source(
      SourceCatalog sourceCatalog,
      HiveConf sourceHiveConf,
      Supplier<CloseableMetaStoreClient> sourceMetaStoreClientSupplier) {
    return new SourceHiveEndpoint(sourceCatalog.getName(), sourceHiveConf, sourceMetaStoreClientSupplier);
  }

  @Bean
  HiveEndpoint replica(
      ReplicaCatalog replicaCatalog,
      HiveConf replicaHiveConf,
      Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier) {
    return new ReplicaHiveEndpoint(replicaCatalog.getName(), replicaHiveConf, replicaMetaStoreClientSupplier);
  }

  @Bean
  ComparisonToolArgs comparisonToolArgs(ApplicationArguments args) {
    return new ComparisonToolArgs(args);
  }

  @Bean
  DiffListener diffListener(ComparisonToolArgs comparisonToolArgs) {
    return new FileOutputDiffListener(comparisonToolArgs.getOutputFile());
  }
}
