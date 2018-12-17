/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
package com.hotels.bdp.circustrain.api.listener;

import org.apache.hadoop.fs.Path;

public interface HousekeepingListener {
  public static final HousekeepingListener NULL = new HousekeepingListener() {

    @Override
    public void cleanUpLocation(
        String eventId,
        String pathEventId,
        Path location,
        String replicaDatabaseName,
        String replicaTableName) {}

  };

  void cleanUpLocation(
      String eventId,
      String pathEventId,
      Path location,
      String replicaDatabaseName,
      String replicaTableName);
}
