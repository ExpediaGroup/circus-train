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

import java.net.URI;
import java.util.List;

public class EventPartition {

  private final List<String> values;
  private final URI location;

  public EventPartition(List<String> values, URI location) {
    this.values = values;
    this.location = location;
  }

  public List<String> getValues() {
    return values;
  }

  public URI getLocation() {
    return location;
  }

}
