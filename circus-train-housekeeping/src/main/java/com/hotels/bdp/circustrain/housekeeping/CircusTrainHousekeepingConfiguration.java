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
package com.hotels.bdp.circustrain.housekeeping;

import org.springframework.boot.orm.jpa.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import com.hotels.bdp.circustrain.api.listener.HousekeepingListener;
import com.hotels.bdp.circustrain.housekeeping.listener.JdbcHousekeepingListener;
import com.hotels.housekeeping.repository.LegacyReplicaPathRepository;
import com.hotels.housekeeping.service.HousekeepingService;
import com.hotels.housekeeping.service.impl.FileSystemHousekeepingService;

@Configuration
@ComponentScan("com.hotels.housekeeping")
@EntityScan(basePackages = { "com.hotels.bdp.circustrain.housekeeping" })
@EnableJpaRepositories(basePackages = { "com.hotels.bdp.circustrain.housekeeping.repository" })
public class CircusTrainHousekeepingConfiguration {

  @Bean
  HousekeepingService housekeepingService(
      LegacyReplicaPathRepository legacyReplicaPathRepository) {
    return new FileSystemHousekeepingService(legacyReplicaPathRepository, new org.apache.hadoop.conf.Configuration());
  }

  @Bean
  HousekeepingListener housekeepingListener() {
    return new JdbcHousekeepingListener();
  }
}
