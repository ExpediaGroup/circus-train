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
package com.hotels.bdp.circustrain.housekeeping;

import java.util.Map;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.circustrain.api.event.HousekeepingListener;
import com.hotels.bdp.circustrain.converter.StringToDurationConverter;
import com.hotels.bdp.circustrain.housekeeping.conf.Housekeeping;
import com.hotels.bdp.circustrain.housekeeping.repository.LegacyReplicaPathRepository;
import com.hotels.bdp.circustrain.housekeeping.service.HousekeepingService;
import com.hotels.bdp.circustrain.housekeeping.service.impl.FileSystemHousekeepingService;

@Configuration
@EnableAutoConfiguration
@EnableConfigurationProperties(Housekeeping.class)
@EnableTransactionManagement
@EnableJpaRepositories(basePackages = { "com.hotels.bdp.circustrain.housekeeping" })
public class HousekeepingConfiguration {

  @Autowired
  ConfigurableEnvironment env;

  @PostConstruct
  public void postConstruct() {
    Map<String, Object> properties = ImmutableMap
        .<String, Object> builder()
        .put("spring.jpa.hibernate.ddl-auto", "update")
        .put("spring.jpa.hibernate.generate-ddl", true)
        .put("spring.jpa.properties.org.hibernate.envers.store_data_at_delete", true)
        .put("spring.jpa.properties.hibernate.listeners.envers.autoRegister", false)
        .put("spring.datasource.initialize", true)
        .put("spring.datasource.max-wait", 10000)
        .put("spring.datasource.max-active", 50)
        .put("spring.datasource.test-on-borrow", true)
        .put("housekeeping.h2.database", "${instance.home}/data/${instance.name}/housekeeping")
        .put("housekeeping.data-source.url",
            "jdbc:h2:${housekeeping.h2.database};AUTO_SERVER=TRUE;DB_CLOSE_ON_EXIT=FALSE")
        .build();
    env.getPropertySources().addLast(new MapPropertySource("housekeepingProperties", properties));
  }

  @Bean
  HousekeepingListener housekeepingListener() {
    return new JdbcHousekeepingListener();
  }

  @Primary
  @Bean(destroyMethod = "close")
  DataSource housekeepingDataSource(Housekeeping housekeeping) {
    return DataSourceBuilder
        .create()
        .driverClassName(housekeeping.getDataSource().getDriverClassName())
        .url(housekeeping.getDataSource().getUrl())
        .username(housekeeping.getDataSource().getUsername())
        .password(housekeeping.getDataSource().getPassword())
        .build();
  }

  @Bean
  HousekeepingService housekeepingService(
      LegacyReplicaPathRepository legacyReplicaPathRepository,
      @Qualifier("replicaHiveConf") org.apache.hadoop.conf.Configuration conf) {
    return new FileSystemHousekeepingService(legacyReplicaPathRepository, conf);
  }

  @Bean
  @ConfigurationPropertiesBinding
  StringToDurationConverter stringToDurationConverter() {
    return new StringToDurationConverter();
  }
}
