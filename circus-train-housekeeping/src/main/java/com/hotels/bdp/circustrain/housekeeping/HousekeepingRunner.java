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

import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.circustrain.api.CompletionCode;
import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.metrics.MetricSender;
import com.hotels.bdp.circustrain.housekeeping.conf.Housekeeping;
import com.hotels.bdp.circustrain.housekeeping.service.HousekeepingService;

@Profile({ Modules.HOUSEKEEPING })
@Component
@Order(Ordered.LOWEST_PRECEDENCE)
class HousekeepingRunner implements ApplicationRunner {
  private static final Logger LOG = LoggerFactory.getLogger(HousekeepingRunner.class);

  private final Housekeeping housekeeping;
  private final HousekeepingService housekeepingService;
  private final MetricSender metricSender;

  @Autowired
  HousekeepingRunner(Housekeeping housekeeping, HousekeepingService housekeepingService, MetricSender metricSender) {
    this.housekeeping = housekeeping;
    this.housekeepingService = housekeepingService;
    this.metricSender = metricSender;
  }

  @Override
  public void run(ApplicationArguments args) {
    Instant deletionCutoff = new Instant().minus(housekeeping.getExpiredPathDuration());
    LOG.info("Housekeeping at instant {} has started", deletionCutoff);
    CompletionCode completionCode = CompletionCode.SUCCESS;
    try {
      housekeepingService.cleanUp(deletionCutoff);
      LOG.info("Housekeeping at instant {} has finished", deletionCutoff);
    } catch (Exception e) {
      completionCode = CompletionCode.FAILURE;
      LOG.error("Housekeeping at instant {} has failed", deletionCutoff, e);
      throw e;
    } finally {
      Map<String, Long> metricsMap = ImmutableMap
          .<String, Long> builder()
          .put("housekeeping", completionCode.getCode())
          .build();
      metricSender.send(metricsMap);
    }
  }

}
