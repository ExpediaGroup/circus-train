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
package com.hotels.bdp.circustrain.core.replica;

import static com.hotels.bdp.circustrain.core.metastore.LocationUtils.locationAsPath;
import static com.hotels.bdp.circustrain.core.replica.TableType.UNPARTITIONED;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.circustrain.api.ReplicaLocationManager;
import com.hotels.bdp.circustrain.api.SourceLocationManager;
import com.hotels.bdp.circustrain.api.event.HousekeepingListener;
import com.hotels.bdp.circustrain.api.event.ReplicaCatalogListener;

public class FullReplicationReplicaLocationManager implements ReplicaLocationManager {

  private static final Logger LOG = LoggerFactory.getLogger(FullReplicationReplicaLocationManager.class);

  private final SourceLocationManager sourceLocationManager;
  private final String tablePath;
  private final String eventId;
  private final List<CleanUpLocation> previousLocations;
  private final TableType tableType;
  private final HousekeepingListener housekeepingListener;
  private final ReplicaCatalogListener replicaCatalogListener;

  FullReplicationReplicaLocationManager(
      SourceLocationManager sourceLocationManager,
      String tablePath,
      String eventId,
      TableType tableType,
      HousekeepingListener housekeepingListener,
      ReplicaCatalogListener replicaCatalogListener) {
    this.sourceLocationManager = sourceLocationManager;
    this.tablePath = tablePath;
    this.eventId = eventId;
    this.tableType = tableType;
    this.replicaCatalogListener = replicaCatalogListener;
    previousLocations = new ArrayList<>();
    this.housekeepingListener = housekeepingListener;
  }

  @Override
  public Path getTableLocation() {
    Path replicaDataLocation = new Path(tablePath);
    if (tableType == UNPARTITIONED) {
      replicaDataLocation = new Path(replicaDataLocation, eventId);
    }
    LOG.debug("Generated table data destination path: {}", replicaDataLocation.toUri());
    replicaCatalogListener.resolvedReplicaLocation(replicaDataLocation.toUri());
    return replicaDataLocation;
  }

  @Override
  public Path getPartitionBaseLocation() {
    if (tableType == UNPARTITIONED) {
      throw new UnsupportedOperationException("Not a partitioned table.");
    }
    Path partitionBasePath = new Path(getTableLocation(), eventId);
    LOG.debug("Generated partition data destination base path: {}", partitionBasePath.toUri());
    return partitionBasePath;
  }

  @Override
  public void addCleanUpLocation(String pathEventId, Path location) {
    LOG.debug("Adding clean up location: {}", location.toUri());
    previousLocations.add(new CleanUpLocation(pathEventId, location));
  }

  @Override
  public void cleanUpLocations() {
    List<URI> uris = new ArrayList<>();
    for (CleanUpLocation location : previousLocations) {
      LOG.info("Scheduling old replica data for deletion for event {}: {}", eventId, location.getPath().toUri());
      housekeepingListener.cleanUpLocation(eventId, location.getPathEventId(), location.getPath());
      uris.add(location.getPath().toUri());
    }
    replicaCatalogListener.deprecatedReplicaLocations(uris);
  }

  @Override
  public Path getPartitionLocation(Partition sourcePartition) {
    if (tableType == UNPARTITIONED) {
      throw new UnsupportedOperationException("Not a partitioned table.");
    }
    Path partitionSubPath = sourceLocationManager.getPartitionSubPath(locationAsPath(sourcePartition));
    Path replicaPartitionLocation = new Path(getPartitionBaseLocation(), partitionSubPath);
    return replicaPartitionLocation;
  }

  @VisibleForTesting
  static class CleanUpLocation {
    private final Path path;
    private final String pathEventId;

    @VisibleForTesting
    CleanUpLocation(String pathEventId, Path path) {
      this.pathEventId = pathEventId;
      this.path = path;
    }

    @VisibleForTesting
    Path getPath() {
      return path;
    }

    @VisibleForTesting
    String getPathEventId() {
      return pathEventId;
    }

  }
}
