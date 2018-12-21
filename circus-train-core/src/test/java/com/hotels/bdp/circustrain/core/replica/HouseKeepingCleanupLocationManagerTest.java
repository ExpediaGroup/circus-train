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
package com.hotels.bdp.circustrain.core.replica;

import static org.mockito.Mockito.verify;

import java.net.URI;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

import com.hotels.bdp.circustrain.api.event.ReplicaCatalogListener;
import com.hotels.bdp.circustrain.api.listener.HousekeepingListener;

@RunWith(MockitoJUnitRunner.class)
public class HouseKeepingCleanupLocationManagerTest {

  private static final String EVENT_ID = "eventId";
  private static final String DATABASE = "db";
  private static final String TABLE = "table1";

  private @Mock HousekeepingListener housekeepingListener;
  private @Mock ReplicaCatalogListener replicaCatalogListener;

  @Test
  public void cleanUpLocations() throws Exception {
    HouseKeepingCleanupLocationManager manager = new HouseKeepingCleanupLocationManager(EVENT_ID, housekeepingListener,
        replicaCatalogListener, DATABASE, TABLE);
    String pathEventId = "pathEventId";
    Path path = new Path("location1");

    manager.addCleanUpLocation(pathEventId, path);
    manager.cleanUpLocations();

    verify(housekeepingListener).cleanUpLocation(EVENT_ID, pathEventId, path, DATABASE, TABLE);
    List<URI> uris = Lists.newArrayList(path.toUri());
    verify(replicaCatalogListener).deprecatedReplicaLocations(uris);
  }

  @Test
  public void cleanUpLocationsMultipleCallsDoNothing() throws Exception {
    HouseKeepingCleanupLocationManager manager = new HouseKeepingCleanupLocationManager(EVENT_ID, housekeepingListener,
        replicaCatalogListener, DATABASE, TABLE);
    String pathEventId = "pathEventId";
    Path path = new Path("location1");

    manager.addCleanUpLocation(pathEventId, path);
    manager.cleanUpLocations();
    manager.cleanUpLocations();
    manager.cleanUpLocations();

    verify(housekeepingListener).cleanUpLocation(EVENT_ID, pathEventId, path, DATABASE, TABLE);
    List<URI> uris = Lists.newArrayList(path.toUri());
    verify(replicaCatalogListener).deprecatedReplicaLocations(uris);
  }

  @Test
  public void cleanUpLocationsMultipleAdds() throws Exception {
    HouseKeepingCleanupLocationManager manager = new HouseKeepingCleanupLocationManager(EVENT_ID, housekeepingListener,
        replicaCatalogListener, DATABASE, TABLE);
    String pathEventId = "pathEventId";
    Path path1 = new Path("location1");
    Path path2 = new Path("location2");

    manager.addCleanUpLocation(pathEventId, path1);
    manager.addCleanUpLocation(pathEventId, path2);
    manager.cleanUpLocations();

    verify(housekeepingListener).cleanUpLocation(EVENT_ID, pathEventId, path1, DATABASE, TABLE);
    verify(housekeepingListener).cleanUpLocation(EVENT_ID, pathEventId, path2, DATABASE, TABLE);
    List<URI> uris = Lists.newArrayList(path1.toUri(), path2.toUri());
    verify(replicaCatalogListener).deprecatedReplicaLocations(uris);
  }

  @Test
  public void cleanUpLocationsMultipleAddsAlternate() throws Exception {
    HouseKeepingCleanupLocationManager manager = new HouseKeepingCleanupLocationManager(EVENT_ID, housekeepingListener,
        replicaCatalogListener, DATABASE, TABLE);
    String pathEventId = "pathEventId";
    Path path1 = new Path("location1");
    Path path2 = new Path("location2");

    manager.addCleanUpLocation(pathEventId, path1);
    manager.cleanUpLocations();
    verify(housekeepingListener).cleanUpLocation(EVENT_ID, pathEventId, path1, DATABASE, TABLE);
    List<URI> uris = Lists.newArrayList(path1.toUri());
    verify(replicaCatalogListener).deprecatedReplicaLocations(uris);

    manager.addCleanUpLocation(pathEventId, path2);
    manager.cleanUpLocations();
    verify(housekeepingListener).cleanUpLocation(EVENT_ID, pathEventId, path2, DATABASE, TABLE);
    List<URI> urisSecondCleanup = Lists.newArrayList(path2.toUri());
    verify(replicaCatalogListener).deprecatedReplicaLocations(urisSecondCleanup);
  }

}
