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

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.hotels.bdp.circustrain.api.event.HousekeepingListener;
import com.hotels.housekeeping.spring.repository.LegacyReplicaPathRepository;
import com.hotels.housekeeping.spring.service.HousekeepingService;
import com.hotels.housekeeping.spring.service.impl.FileSystemHousekeepingService;

@Configuration
@ComponentScan("com.hotels.housekeeping")
public class CircusTrainHousekeepingConfiguration {

  @Bean
  HousekeepingListener housekeepingListener() {
    return new JdbcHousekeepingListener();
  }

  @Bean
  HousekeepingService housekeepingService(
      LegacyReplicaPathRepository legacyReplicaPathRepository,
      @Qualifier("replicaHiveConf") org.apache.hadoop.conf.Configuration conf) {
    return new FileSystemHousekeepingService(legacyReplicaPathRepository, conf);
  }
}
