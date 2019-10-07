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
package com.hotels.bdp.circustrain.api.event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

public class EventPartitions {

  private List<EventPartition> eventPartitions = new ArrayList<>();
  private final LinkedHashMap<String, String> partitionKeyTypes;

  public EventPartitions(LinkedHashMap<String, String> partitionKeyTypes) {
    this.partitionKeyTypes = partitionKeyTypes;
  }

  public boolean add(EventPartition eventPartition) {
    return eventPartitions.add(eventPartition);
  }

  public List<EventPartition> getEventPartitions() {
    return Collections.unmodifiableList(eventPartitions);
  }

  public LinkedHashMap<String, String> getPartitionKeyTypes() {
    return partitionKeyTypes;
  }

}
