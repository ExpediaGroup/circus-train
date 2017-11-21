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
package com.hotels.bdp.circustrain.api.event;

import java.util.List;
import java.util.Map;

public class EventSourceCatalog {

  private final String name;
  private final boolean disableSnapshots;
  private final List<String> siteXml;
  private final Map<String, String> configurationProperties;

  public EventSourceCatalog(
      String name,
      boolean disableSnapshots,
      List<String> siteXml,
      Map<String, String> configurationProperties) {
    this.name = name;
    this.disableSnapshots = disableSnapshots;
    this.siteXml = siteXml;
    this.configurationProperties = configurationProperties;
  }

  public String getName() {
    return name;
  }

  public boolean isDisableSnapshots() {
    return disableSnapshots;
  }

  public List<String> getSiteXml() {
    return siteXml;
  }

  public Map<String, String> getConfigurationProperties() {
    return configurationProperties;
  }

}
