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
package com.hotels.bdp.circustrain.api.copier;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.circustrain.api.metrics.Metrics;

@RunWith(MockitoJUnitRunner.class)
public class MetricsMergerTest {

  private @Mock Metrics firstMetrics;
  private @Mock Metrics secondMetrics;

  @Before
  public void init() {
    when(firstMetrics.getMetrics()).thenReturn(ImmutableMap.of("first", 1L, "common", 5L));
    when(firstMetrics.getBytesReplicated()).thenReturn(1L);

    when(secondMetrics.getMetrics()).thenReturn(ImmutableMap.of("second", 2L, "common", 3L));
    when(secondMetrics.getBytesReplicated()).thenReturn(2L);
  }

  @Test
  public void summary() {
    Metrics mergedMetrics = MetricsMerger.DEFAULT.merge(firstMetrics, secondMetrics);
    assertThat(mergedMetrics, is(notNullValue()));
    assertThat(mergedMetrics.getMetrics().size(), is(3));
    assertThat(mergedMetrics.getMetrics().get("first"), is(1L));
    assertThat(mergedMetrics.getMetrics().get("second"), is(2L));
    assertThat(mergedMetrics.getMetrics().get("common"), is(8L));
    assertThat(mergedMetrics.getBytesReplicated(), is(3L));
  }

}
