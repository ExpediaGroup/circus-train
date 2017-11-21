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
package com.hotels.bdp.circustrain.converter;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.joda.time.Duration;
import org.junit.Test;

// Convert HH:MM:SS to seconds -> https://www.tools4noobs.com/online_tools/hh_mm_ss_to_seconds/
public class StringToDurationConverterTest {

  private final StringToDurationConverter converter = new StringToDurationConverter();

  @Test
  public void convertFullFormat() {
    Duration duration = converter.convert("P15DT12H35M1S");
    assertThat(duration.getMillis(), is(1341301000L));
  }

  @Test
  public void convertDays() {
    Duration duration = converter.convert("P7D");
    assertThat(duration.getMillis(), is(604800000L));
  }

  @Test
  public void convertTime() {
    Duration duration = converter.convert("PT168H13M1S");
    assertThat(duration.getMillis(), is(605581000L));
  }

  @Test
  public void convertHours() {
    Duration duration = converter.convert("P1H");
    assertThat(duration.getMillis(), is(3600000L));
  }

  @Test
  public void convertMinutes() {
    Duration duration = converter.convert("P20M");
    assertThat(duration.getMillis(), is(1200000L));
  }

  @Test
  public void convertSeconds() {
    Duration duration = converter.convert("P15S");
    assertThat(duration.getMillis(), is(15000L));
  }

  @Test(expected = IllegalArgumentException.class)
  public void missingPeriodLiteral() {
    converter.convert("7D");
  }

  @Test(expected = IllegalArgumentException.class)
  public void illegalTimePrefix() {
    converter.convert("P7DT");
  }

}
