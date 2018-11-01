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
  private final List<CleanUpLocation> locations = new ArrayList<>();

  public HouseKeepingCleanupLocationManager(
      String eventId,
      HousekeepingListener housekeepingListener,
      ReplicaCatalogListener replicaCatalogListener) {
    this.eventId = eventId;
    this.housekeepingListener = housekeepingListener;
    this.replicaCatalogListener = replicaCatalogListener;
  }

  @Override
  public void cleanUpLocations() throws CircusTrainException {
    try {
      List<URI> uris = new ArrayList<>();
      for (CleanUpLocation location : locations) {
        LOG.info("Scheduling old replica data for deletion for event {}: {}", eventId, location.getPath().toUri());
        housekeepingListener.cleanUpLocation(eventId, location.getPathEventId(), location.getPath());
        uris.add(location.getPath().toUri());
      }
      replicaCatalogListener.deprecatedReplicaLocations(uris);
    } finally {
      locations.clear();
    }
  }

  @Override
  public void addCleanUpLocation(String pathEventId, Path location) {
    LOG.debug("Adding clean up location: {}", location.toUri());
    locations.add(new CleanUpLocation(pathEventId, location));
  }

  private static class CleanUpLocation {
    private final Path path;
    private final String pathEventId;

    private CleanUpLocation(String pathEventId, Path path) {
      this.pathEventId = pathEventId;
      this.path = path;
    }

    private Path getPath() {
      return path;
    }

    private String getPathEventId() {
      return pathEventId;
    }

  }
}
