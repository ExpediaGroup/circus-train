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
package com.hotels.bdp.circustrain.housekeeping.service;

import org.joda.time.Instant;

import com.hotels.bdp.circustrain.housekeeping.model.LegacyReplicaPath;

public interface HousekeepingService {

  /**
   * Deletes all paths from the file system which are older than {@code referenceTime}.
   *
   * @param referenceTime path deletion reference time
   */
  void cleanUp(Instant referenceTime);

  /**
   * Schedules a file system path for deletion.
   *
   * @param cleanUpPath file system path encapsulation
   */
  void scheduleForHousekeeping(LegacyReplicaPath cleanUpPath);

}
