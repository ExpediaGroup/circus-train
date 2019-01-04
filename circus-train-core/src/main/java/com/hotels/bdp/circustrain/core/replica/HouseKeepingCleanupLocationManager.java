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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.event.ReplicaCatalogListener;
import com.hotels.bdp.circustrain.api.listener.HousekeepingListener;

public class HouseKeepingCleanupLocationManager implements CleanupLocationManager {

  private final static Logger LOG = LoggerFactory.getLogger(HouseKeepingCleanupLocationManager.class);

  private final String eventId;
  private final HousekeepingListener housekeepingListener;
  private final ReplicaCatalogListener replicaCatalogListener;
  private final List<CleanupLocation> locations = new ArrayList<>();

  private final String replicaDatabase;

  private final String replicaTable;

  public HouseKeepingCleanupLocationManager(
      String eventId,
      HousekeepingListener housekeepingListener,
      ReplicaCatalogListener replicaCatalogListener,
      String replicaDatabase,
      String replicaTable) {
    this.eventId = eventId;
    this.housekeepingListener = housekeepingListener;
    this.replicaCatalogListener = replicaCatalogListener;
    this.replicaDatabase = replicaDatabase;
    this.replicaTable = replicaTable;
  }

  @Override
  public void scheduleLocations() throws CircusTrainException {
    try {
      List<URI> uris = new ArrayList<>();
      for (CleanupLocation location : locations) {
        LOG.info("Scheduling old replica data for deletion for event {}: {}", eventId, location.path.toUri());
        housekeepingListener
            .cleanUpLocation(eventId, location.pathEventId, location.path, location.replicaDatabase,
                location.replicaTable);
        uris.add(location.path.toUri());
      }
      replicaCatalogListener.deprecatedReplicaLocations(uris);
    } finally {
      locations.clear();
    }
  }

  @Override
  public void addCleanupLocation(String pathEventId, Path location) {
    LOG.debug("Adding clean up location: {}", location.toUri());
    locations.add(new CleanupLocation(pathEventId, location, replicaDatabase, replicaTable));
  }

  private static class CleanupLocation {
    private final Path path;
    private final String pathEventId;
    private final String replicaDatabase;
    private final String replicaTable;

    private CleanupLocation(String pathEventId, Path path, String replicaDatabase, String replicaTable) {
      this.pathEventId = pathEventId;
      this.path = path;
      this.replicaDatabase = replicaDatabase;
      this.replicaTable = replicaTable;
    }

  }
}
