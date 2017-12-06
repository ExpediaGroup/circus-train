package com.hotels.bdp.circustrain.housekeeping.listener;

import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;

import com.hotels.bdp.circustrain.api.listener.HousekeepingListener;
import com.hotels.bdp.housekeeping.api.model.DefaultLegacyReplicaPath;
import com.hotels.bdp.housekeeping.api.service.HousekeepingService;

public class JdbcHousekeepingListener implements HousekeepingListener {

  @Autowired
  private HousekeepingService cleanUpPathService;

  @Override
  public void cleanUpLocation(String eventId, String pathEventId, Path location) {
    cleanUpPathService
        .scheduleForHousekeeping(new DefaultLegacyReplicaPath(eventId, pathEventId, location.toUri().toString()));
  }
}
