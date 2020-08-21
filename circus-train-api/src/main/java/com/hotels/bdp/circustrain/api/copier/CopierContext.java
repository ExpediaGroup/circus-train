/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
package com.hotels.bdp.circustrain.api.copier;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import com.hotels.bdp.circustrain.api.conf.TableReplication;

public class CopierContext {

  private String eventId;
  private Path sourceBaseLocation;
  private List<Path> sourceSubLocations = Collections.<Path>emptyList();
  private Path replicaLocation;
  private Map<String, Object> copierOptions;
  private TableReplication tableReplication;

  public CopierContext(
      String eventId,
      Path sourceBaseLocation,
      List<Path> sourceSubLocations,
      Path replicaLocation,
      Map<String, Object> copierOptions) {
    this.eventId = eventId;
    this.sourceBaseLocation = sourceBaseLocation;
    this.sourceSubLocations = sourceSubLocations;
    this.replicaLocation = replicaLocation;
    this.copierOptions = copierOptions;
  }

  public CopierContext(
      String eventId,
      Path sourceBaseLocation,
      Path replicaLocation,
      Map<String, Object> copierOptions) {
    this.eventId = eventId;
    this.sourceBaseLocation = sourceBaseLocation;
    this.replicaLocation = replicaLocation;
    this.copierOptions = copierOptions;
  }

  public String getEventId() {
    return eventId;
  }

  public void setEventId(String eventId) {
    this.eventId = eventId;
  }

  public Path getSourceBaseLocation() {
    return sourceBaseLocation;
  }

  public void setSourceBaseLocation(Path sourceBaseLocation) {
    this.sourceBaseLocation = sourceBaseLocation;
  }

  public List<Path> getSourceSubLocations() {
    return sourceSubLocations;
  }

  public void setSourceSubLocations(List<Path> sourceSubLocations) {
    this.sourceSubLocations = sourceSubLocations;
  }

  public Path getReplicaLocation() {
    return replicaLocation;
  }

  public void setReplicaLocation(Path replicaLocation) {
    this.replicaLocation = replicaLocation;
  }

  public Map<String, Object> getCopierOptions() {
    return copierOptions;
  }

  public void setCopierOptions(Map<String, Object> copierOptions) {
    this.copierOptions = copierOptions;
  }

  public void setTableReplication(TableReplication tableReplication) {
    this.tableReplication = tableReplication;
  }

  public TableReplication getTableReplication() {
    return tableReplication;
  }

}
