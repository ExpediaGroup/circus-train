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

public interface CopierFactory {

  boolean supportsSchemes(String sourceScheme, String replicaScheme);

  /**
   * Creates a new Copier.
   * 
   * @param copierContext Context object containing configuration values for the Copier.
   * @return
   */
  default Copier newInstance(CopierContext copierContext) {
    //TODO: this is only here for backwards compatibility with CopierFactorys using older versions of Circus Train, when the below
    //deprecated methods are removed so should this default implementation
    return newInstance(copierContext.getEventId(), copierContext.getSourceBaseLocation(), copierContext.getSourceSubLocations(), copierContext.getReplicaLocation(), 
        copierContext.getCopierOptions());
  }

  /**
   * @deprecated As of release 16.3.0, replaced by {@link #newInstance(CopierContext)}.
   * @param eventId
   * @param sourceBaseLocation
   * @param sourceSubLocations
   * @param replicaLocation
   * @param copierOptions, contains both global and per table override configured options
   * @return
   */
  @Deprecated
  Copier newInstance(
      String eventId,
      Path sourceBaseLocation,
      List<Path> sourceSubLocations,
      Path replicaLocation,
      Map<String, Object> copierOptions);

  /**
   * @deprecated As of release 16.3.0, replaced by {@link #newInstance(CopierContext)}.
   * 
   * @param eventId
   * @param sourceBaseLocation
   * @param replicaLocation
   * @param copierOptions, contains both global and per table override configured options
   * @return
   */
  @Deprecated
  Copier newInstance(String eventId, Path sourceBaseLocation, Path replicaLocation, Map<String, Object> copierOptions);

}
