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
package com.hotels.bdp.circustrain.aws.sns.event;

import java.util.Map;

import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "sns-event-listener")
public class ListenerConfig {

  private String startTopic;
  private String successTopic;
  private String failTopic;
  private String topic;
  private String subject;
  @NotNull
  private String region;
  private Map<String, String> headers;
  private int queueSize = 100;

  public void setQueueSize(int queueSize) {
    this.queueSize = queueSize;
  }

  public String getStartTopic() {
    return startTopic == null ? topic : startTopic;
  }

  public void setStartTopic(String startTopic) {
    this.startTopic = startTopic;
  }

  public String getSuccessTopic() {
    return successTopic == null ? topic : successTopic;
  }

  public void setSuccessTopic(String successTopic) {
    this.successTopic = successTopic;
  }

  public String getFailTopic() {
    return failTopic == null ? topic : failTopic;
  }

  public void setFailTopic(String failTopic) {
    this.failTopic = failTopic;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public void setHeaders(Map<String, String> headers) {
    this.headers = headers;
  }

  public String getSubject() {
    return subject;
  }

  public void setSubject(String subject) {
    this.subject = subject;
  }

  public int getQueueSize() {
    return queueSize;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = region;
  }

}
