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
package com.hotels.bdp.circustrain.s3s3copier;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.circustrain.api.metrics.Metrics;

public class S3S3CopierMetrics implements Metrics {

  public static enum Metrics {
    TOTAL_BYTES_TO_REPLICATE;
  }

  private final long bytesReplicated;
  private final Map<String, Long> metrics;

  public S3S3CopierMetrics(Map<String, Long> metrics, long bytesReplicated) {
    this.metrics = metrics;
    this.bytesReplicated = bytesReplicated;
  }

  @Override
  public Map<String, Long> getMetrics() {
    if (metrics != null) {
      return metrics;
    }
    return ImmutableMap.of();
  }

  @Override
  public long getBytesReplicated() {
    return bytesReplicated;
  }

}
