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
package com.hotels.bdp.circustrain;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Profile;
import org.springframework.validation.BindException;
import org.springframework.validation.ObjectError;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;

import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.copier.CopierOptions;
import com.hotels.bdp.circustrain.api.event.CopierListener;
import com.hotels.bdp.circustrain.api.event.LocomotiveListener;
import com.hotels.bdp.circustrain.api.event.ReplicaCatalogListener;
import com.hotels.bdp.circustrain.api.event.SourceCatalogListener;
import com.hotels.bdp.circustrain.api.event.TableReplicationListener;
import com.hotels.bdp.circustrain.api.listener.HousekeepingListener;
import com.hotels.bdp.circustrain.api.metadata.ColumnStatisticsTransformation;
import com.hotels.bdp.circustrain.api.metadata.PartitionTransformation;
import com.hotels.bdp.circustrain.api.metadata.TableTransformation;
import com.hotels.bdp.circustrain.api.metrics.LoggingScheduledReporterFactory;
import com.hotels.bdp.circustrain.api.metrics.MetricSender;
import com.hotels.bdp.circustrain.api.metrics.ScheduledReporterFactory;
import com.hotels.bdp.circustrain.comparator.hive.functions.PathDigest;
import com.hotels.bdp.circustrain.comparator.hive.functions.PathToPathMetadata;
import com.hotels.bdp.circustrain.core.CopierFactoryManager;
import com.hotels.bdp.circustrain.core.PartitionPredicateFactory;
import com.hotels.bdp.circustrain.core.ReplicationFactory;
import com.hotels.bdp.circustrain.core.ReplicationFactoryImpl;
import com.hotels.bdp.circustrain.core.conf.SpringExpressionParser;
import com.hotels.bdp.circustrain.core.event.CompositeCopierListener;
import com.hotels.bdp.circustrain.core.event.CompositeLocomotiveListener;
import com.hotels.bdp.circustrain.core.event.CompositeReplicaCatalogListener;
import com.hotels.bdp.circustrain.core.event.CompositeSourceCatalogListener;
import com.hotels.bdp.circustrain.core.event.CompositeTableReplicationListener;
import com.hotels.bdp.circustrain.core.replica.ReplicaFactory;
import com.hotels.bdp.circustrain.core.source.SourceFactory;
import com.hotels.bdp.circustrain.core.transformation.CompositePartitionTransformation;
import com.hotels.bdp.circustrain.core.transformation.CompositeTableTransformation;
import com.hotels.bdp.circustrain.extension.ExtensionInitializer;
import com.hotels.bdp.circustrain.manifest.ManifestAttributes;

@SpringBootApplication
@EnableConfigurationProperties
@ComponentScan(basePackages = {
    "com.hotels.bdp.circustrain.avro",
    "com.hotels.bdp.circustrain.aws",
    "com.hotels.bdp.circustrain.context",
    "com.hotels.bdp.circustrain.core",
    "com.hotels.bdp.circustrain.distcpcopier",
    "com.hotels.bdp.circustrain.gcp",
    "com.hotels.bdp.circustrain.hive.view.transformation",
    "com.hotels.bdp.circustrain.housekeeping",
    "com.hotels.bdp.circustrain.metrics.conf",
    "com.hotels.bdp.circustrain.s3mapreducecpcopier",
    "com.hotels.bdp.circustrain.s3s3copier" })
public class CircusTrain {
  private static final Logger LOG = LoggerFactory.getLogger(CircusTrain.class);

  public static void main(String[] args) throws Exception {
    // below is output *before* logging is configured so will appear on console
    logVersionInfo();

    int exitCode = -1;
    try {
      String defaultModules = Joiner.on(",").join(Modules.REPLICATION, Modules.HOUSEKEEPING);
      exitCode = SpringApplication
          .exit(new SpringApplicationBuilder(CircusTrain.class)
              .properties("spring.config.location:${config:null}")
              .properties("spring.profiles.active:${modules:" + defaultModules + "}")
              .properties("instance.home:${user.home}")
              .properties("instance.name:${source-catalog.name}_${replica-catalog.name}")
              .properties("jasypt.encryptor.password:${password:null}")
              .properties("housekeeping.schema-name:circus_train")
              .registerShutdownHook(true)
              .initializers(new ExtensionInitializer())
              .listeners(new ConfigFileValidationApplicationListener())
              .build()
              .run(args));
    } catch (ConfigFileValidationException e) {
      LOG.error(e.getMessage(), e);
      printCircusTrainHelp(e.getErrors());
    } catch (BeanCreationException e) {
      LOG.error(e.getMessage(), e);
      if (e.getMostSpecificCause() instanceof BindException) {
        printCircusTrainHelp(((BindException) e.getMostSpecificCause()).getAllErrors());
      }
    }

    System.exit(exitCode);
  }

  private static void printCircusTrainHelp(List<ObjectError> allErrors) {
    System.out.println(new CircusTrainHelp(allErrors));
  }

  CircusTrain() {
    // below is output *after* logging is configured so will appear in log file
    logVersionInfo();
  }

  private static void logVersionInfo() {
    ManifestAttributes manifestAttributes = new ManifestAttributes(CircusTrain.class);
    LOG.info("{}", manifestAttributes);
  }

  @Bean
  LocomotiveListener locomotiveListener(List<LocomotiveListener> listeners) {
    if (listeners == null) {
      listeners = Collections.emptyList();
    }
    return new CompositeLocomotiveListener(listeners);
  }

  @Bean
  SourceCatalogListener sourceCatalogListener(List<SourceCatalogListener> listeners) {
    if (listeners == null) {
      listeners = Collections.emptyList();
    }
    return new CompositeSourceCatalogListener(listeners);
  }

  @Bean
  ReplicaCatalogListener replicaCatalogListener(List<ReplicaCatalogListener> listeners) {
    if (listeners == null) {
      listeners = Collections.emptyList();
    }
    return new CompositeReplicaCatalogListener(listeners);
  }

  @Bean
  TableReplicationListener tableReplicationListener(List<TableReplicationListener> listeners) {
    if (listeners == null) {
      listeners = Collections.emptyList();
    }
    return new CompositeTableReplicationListener(listeners);
  }

  @Bean
  CopierListener copierListener(List<CopierListener> listeners) {
    if (listeners == null) {
      listeners = Collections.emptyList();
    }
    return new CompositeCopierListener(listeners);
  }

  @Profile({ Modules.REPLICATION })
  @Bean
  TableTransformation tableTransformation(List<TableTransformation> transformations) {
    if (transformations == null) {
      transformations = Collections.emptyList();
    }
    return new CompositeTableTransformation(transformations);
  }

  @Profile({ Modules.REPLICATION })
  @Bean
  PartitionTransformation partitionTransformation(List<PartitionTransformation> transformations) {
    if (transformations == null) {
      transformations = Collections.emptyList();
    }
    return new CompositePartitionTransformation(transformations);
  }

  @Bean
  @ConditionalOnMissingBean(ColumnStatisticsTransformation.class)
  ColumnStatisticsTransformation columnStatisticsTransformation() {
    return ColumnStatisticsTransformation.IDENTITY;
  }

  @Bean
  @ConditionalOnMissingBean(MetricSender.class)
  MetricSender metricSender() {
    return MetricSender.DEFAULT_LOG_ONLY;
  }

  @Bean
  @ConditionalOnMissingBean(value = ScheduledReporterFactory.class)
  ScheduledReporterFactory runningScheduledReporterFactory(MetricRegistry runningMetricRegistry) {
    return new LoggingScheduledReporterFactory(runningMetricRegistry);
  }

  @Bean
  MetricRegistry runningMetricRegistry() {
    return new MetricRegistry();
  }

  @Bean
  @ConditionalOnMissingBean(name = "housekeepingListener")
  HousekeepingListener housekeepingListener() {
    return HousekeepingListener.NULL;
  }

  @Profile({ Modules.REPLICATION })
  @Bean
  ReplicationFactory replicationFactory(
      SourceFactory sourceFactory,
      ReplicaFactory replicaFactory,
      CopierFactoryManager copierFactoryManager,
      CopierListener copierListener,
      PartitionPredicateFactory partitionPredicateFactory,
      CopierOptions copierOptions) {
    return new ReplicationFactoryImpl(sourceFactory, replicaFactory, copierFactoryManager, copierListener,
        partitionPredicateFactory, copierOptions);
  }

  @Profile({ Modules.REPLICATION })
  @Bean
  PartitionPredicateFactory partitionPredicateFactory(
      SourceFactory sourceFactory,
      ReplicaFactory replicaFactory,
      SpringExpressionParser expressionParser,
      @Value("#{checksumFunction}") Function<Path, String> checksumFunction) {
    return new PartitionPredicateFactory(sourceFactory, replicaFactory, expressionParser, checksumFunction);
  }

  @Profile({ Modules.REPLICATION })
  @Bean
  Function<Path, String> checksumFunction(HiveConf sourceHiveConf) {
    return Functions.compose(new PathDigest(), new PathToPathMetadata(sourceHiveConf));
  }
}
