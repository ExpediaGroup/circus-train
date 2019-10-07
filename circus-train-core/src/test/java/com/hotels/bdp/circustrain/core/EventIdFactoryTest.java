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
package com.hotels.bdp.circustrain.core;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class EventIdFactoryTest {

  @Test
  public void checkPrefix() {
    String id1 = EventIdFactory.DEFAULT.newEventId("X");
    String id2 = EventIdFactory.DEFAULT.newEventId("Y");
    assertThat(id1.startsWith("x-"), is(true));
    assertThat(id2.startsWith("y-"), is(true));
  }

  /**
   * Clearly we cannot conclusively check for uniqueness, instead this test is here to check that we at least don't have
   * something that is simply an identity function. Yes, we could make some statistical inference, but we won't.
   */
  @Test
  public void checkDifferent() {
    String id1 = EventIdFactory.DEFAULT.newEventId("X");
    String id2 = EventIdFactory.DEFAULT.newEventId("X");
    assertThat(id1.equals(id2), is(false));
  }

  @Test
  public void checkLowerCase() {
    String id = EventIdFactory.DEFAULT.newEventId("X");
    assertThat(id, is(id.toLowerCase()));
  }
}
