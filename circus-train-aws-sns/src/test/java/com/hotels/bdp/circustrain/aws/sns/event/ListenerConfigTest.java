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
package com.hotels.bdp.circustrain.aws.sns.event;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class ListenerConfigTest {

  private final ListenerConfig config = new ListenerConfig();

  @Test
  public void defaultQueueSize() {
    assertThat(config.getQueueSize(), is(100));
  }

  @Test
  public void successTopicDefaults() {
    config.setTopic("x");
    assertThat(config.getSuccessTopic(), is("x"));
  }

  @Test
  public void successTopic() {
    config.setTopic("x");
    config.setSuccessTopic("y");
    assertThat(config.getSuccessTopic(), is("y"));
  }

  @Test
  public void failTopicDefaults() {
    config.setTopic("x");
    assertThat(config.getFailTopic(), is("x"));
  }

  @Test
  public void failTopic() {
    config.setTopic("x");
    config.setFailTopic("y");
    assertThat(config.getFailTopic(), is("y"));
  }

  @Test
  public void startTopicDefaults() {
    config.setTopic("x");
    assertThat(config.getStartTopic(), is("x"));
  }

  @Test
  public void startTopic() {
    config.setTopic("x");
    config.setStartTopic("y");
    assertThat(config.getStartTopic(), is("y"));
  }
}
