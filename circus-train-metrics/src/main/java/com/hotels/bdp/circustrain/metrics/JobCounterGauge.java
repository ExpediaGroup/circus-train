/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.bdp.circustrain.metrics;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;

public class JobCounterGauge implements Gauge<Long> {

  private static final Logger LOG = LoggerFactory.getLogger(JobCounterGauge.class);

  private final Job job;
  private final Enum<?> counter;
  private final String counterName;
  private final String groupName;

  public JobCounterGauge(Job job, String groupName, String counterName) {
    this(job, null, groupName, counterName);
  }

  public JobCounterGauge(Job job, Enum<?> counter) {
    this(job, counter, null, null);
  }

  private JobCounterGauge(Job job, Enum<?> counter, String groupName, String counterName) {
    this.job = job;
    this.counter = counter;
    this.groupName = groupName;
    this.counterName = counterName;
  }

  @Override
  public Long getValue() {
    try {
      if (groupName != null) {
        return job.getCounters().findCounter(groupName, counterName).getValue();
      } else {
        return job.getCounters().findCounter(counter).getValue();
      }
    } catch (IOException e) {
      LOG.warn("Could not get value for counter " + counter, e);
    }
    return 0L;
  }
}
