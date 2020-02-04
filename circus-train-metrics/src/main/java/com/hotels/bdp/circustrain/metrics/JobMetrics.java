/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
import java.util.Map;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.metrics.Metrics;
import com.hotels.bdp.circustrain.api.util.DotJoiner;

public class JobMetrics implements Metrics {

  private final Map<String, Long> metrics;
  private final Long bytesReplicated;

  public JobMetrics(Job job, String bytesReplicatedGroup, String bytesReplicatedCounter) {
    this(job, DotJoiner.join(bytesReplicatedGroup, bytesReplicatedCounter));
  }

  public JobMetrics(Job job, Enum<?> bytesReplicatedCounter) {
    this(job, bytesReplicatedCounter.getDeclaringClass().getName(), bytesReplicatedCounter.name());
  }

  public JobMetrics(Job job, String bytesReplicatedKey) {
    Builder<String, Long> builder = ImmutableMap.builder();
    if (job != null) {
      Counters counters;
      try {
        counters = job.getCounters();
      } catch (IOException e) {
        throw new CircusTrainException("Unable to get counters from job.", e);
      }
      if (counters != null) {
        for (CounterGroup group : counters) {
          for (Counter counter : group) {
            builder.put(DotJoiner.join(group.getName(), counter.getName()), counter.getValue());
          }
        }
      }
    }
    metrics = builder.build();
    Long bytesReplicatedValue = metrics.get(bytesReplicatedKey);
    if (bytesReplicatedValue != null) {
      bytesReplicated = bytesReplicatedValue;
    } else {
      bytesReplicated = 0L;
    }
  }

  @Override
  public Map<String, Long> getMetrics() {
    return metrics;
  }

  @Override
  public long getBytesReplicated() {
    return bytesReplicated;
  }

}
