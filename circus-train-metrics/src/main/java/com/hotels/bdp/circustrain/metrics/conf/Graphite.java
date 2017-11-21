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
package com.hotels.bdp.circustrain.metrics.conf;

import javax.annotation.PostConstruct;
import javax.validation.constraints.Pattern;

import org.apache.hadoop.fs.Path;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "graphite")
class Graphite {

  private boolean enabled = false;
  private Path config;
  @Pattern(regexp = "[^:]+:[0-9]+")
  private String host;
  private String prefix;
  private String namespace;

  @PostConstruct
  void init() {
    if (config != null || host != null || prefix != null || namespace != null) {
      enabled = true;
    }
  }

  public boolean isEnabled() {
    return enabled;
  }

  public Path getConfig() {
    return config;
  }

  public void setConfig(Path config) {
    this.config = config;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

}
