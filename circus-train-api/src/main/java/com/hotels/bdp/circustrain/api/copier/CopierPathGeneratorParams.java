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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class CopierPathGeneratorParams {

  //TODO: should we change this to take the CopierContext instead of the arguments after copierIndex?
  public static CopierPathGeneratorParams newParams(
      int copierIndex,
      String eventId,
      Path originalSourceBaseLocation,
      List<Path> originalSourceSubLocations,
      Path originalReplicaLocation,
      Map<String, Object> overridingCopierOptions) {
    return new CopierPathGeneratorParams(copierIndex, eventId, originalSourceBaseLocation, originalSourceSubLocations,
        originalReplicaLocation, overridingCopierOptions);
  }

  private final int copierIndex;
  private final String eventId;
  private final Path originalSourceBaseLocation;
  private final List<Path> originalSourceSubLocations;
  private final Path originalReplicaLocation;
  private final Map<String, Object> overridingCopierOptions;

  private CopierPathGeneratorParams(
      int copierIndex,
      String eventId,
      Path originalSourceBaseLocation,
      List<Path> originalSourceSubLocations,
      Path originalReplicaLocation,
      Map<String, Object> overridingCopierOptions) {
    this.copierIndex = copierIndex;
    this.eventId = eventId;
    this.originalSourceBaseLocation = originalSourceBaseLocation;
    this.originalSourceSubLocations = originalSourceSubLocations;
    this.originalReplicaLocation = originalReplicaLocation;
    this.overridingCopierOptions = overridingCopierOptions;
  }

  public int getCopierIndex() {
    return copierIndex;
  }

  public String getEventId() {
    return eventId;
  }

  public Path getOriginalSourceBaseLocation() {
    return originalSourceBaseLocation;
  }

  public List<Path> getOriginalSourceSubLocations() {
    return ImmutableList.copyOf(originalSourceSubLocations);
  }

  public Path getOriginalReplicaLocation() {
    return originalReplicaLocation;
  }

  public Map<String, Object> getOverridingCopierOptions() {
    return ImmutableMap.copyOf(overridingCopierOptions);
  }

}
