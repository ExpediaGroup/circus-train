package com.hotels.bdp.circustrain.core.replica;

import org.apache.hadoop.fs.Path;

import com.hotels.bdp.circustrain.api.CircusTrainException;

public interface CleanupLocationManager {

  CleanupLocationManager NULL_CLEANUP_LOCATION_MANAGER = new CleanupLocationManager() {

    @Override
    public void cleanUpLocations() throws CircusTrainException {
      // do nothing
    }

    @Override
    public void addCleanUpLocation(String pathEventId, Path location) {
      // do nothing
    }
  };

  /**
   * Cleans up locations added by {@link #addCleanUpLocation(String, Path)}
   *
   * @throws CircusTrainException
   */
  void cleanUpLocations() throws CircusTrainException;

  void addCleanUpLocation(String pathEventId, Path location);

}
