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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.metrics.MetricSender;
import com.hotels.bdp.circustrain.housekeeping.conf.Housekeeping;
import com.hotels.bdp.circustrain.housekeeping.service.HousekeepingService;

@RunWith(MockitoJUnitRunner.class)
public class HousekeepingRunnerTest {
  private static final Duration TWO_DAYS_DURATION = Duration.standardDays(2);

  private @Mock Housekeeping housekeeping;
  private @Mock HousekeepingService cleanUpPathService;
  private @Mock MetricSender metricSender;

  private HousekeepingRunner runner;

  @Before
  public void init() {
    when(housekeeping.getExpiredPathDuration()).thenReturn(TWO_DAYS_DURATION);
    runner = new HousekeepingRunner(housekeeping, cleanUpPathService, metricSender);
  }

  @Test
  public void typical() throws Exception {
    ArgumentCaptor<Instant> instantCaptor = ArgumentCaptor.forClass(Instant.class);
    runner.run(null);
    verify(cleanUpPathService).cleanUp(instantCaptor.capture());
    Instant twoDaysAgo = new Instant().minus(TWO_DAYS_DURATION.getMillis());
    assertThat(instantCaptor.getValue().getMillis(), is(lessThanOrEqualTo(twoDaysAgo.getMillis())));
  }

  @Test(expected = IllegalStateException.class)
  public void rethrowException() throws Exception {
    doThrow(new IllegalStateException()).when(cleanUpPathService).cleanUp(any(Instant.class));
    runner.run(null);
  }

}
