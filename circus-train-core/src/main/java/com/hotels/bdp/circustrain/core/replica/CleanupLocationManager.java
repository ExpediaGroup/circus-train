/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.bdp.circustrain.core.replica;

import org.apache.hadoop.fs.Path;

import com.hotels.bdp.circustrain.api.CircusTrainException;

public interface CleanupLocationManager {

  CleanupLocationManager NULL_CLEANUP_LOCATION_MANAGER = new CleanupLocationManager() {

    @Override
    public void scheduleLocations() throws CircusTrainException {
      // do nothing
    }

    @Override
    public void addCleanupLocation(String pathEventId, Path location) {
      // do nothing
    }
  };

  /**
   * Schedules locations for cleanup, locations can be added by {@link #addCleanupLocation(String, Path)}
   *
   * @throws CircusTrainException
   */
  void scheduleLocations() throws CircusTrainException;

  void addCleanupLocation(String pathEventId, Path location);

}
