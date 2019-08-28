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
package com.hotels.bdp.circustrain.core.transformation;

import java.util.Collections;
import java.util.Map;

import com.hotels.bdp.circustrain.api.event.EventTableReplication;
import com.hotels.bdp.circustrain.api.event.TableReplicationListener;
import com.hotels.bdp.circustrain.core.conf.TableParametersConfig;

public abstract class AbstractTableParametersTransformation implements TableReplicationListener {

  private final TableParametersConfig tableParametersConfig;
  private Map<String, String> tableParametersConfigOverride;

  protected AbstractTableParametersTransformation(TableParametersConfig tableParametersConfig) {
    this.tableParametersConfig = tableParametersConfig;
  }

  protected Map<String, String> getTableParameters() {
    if (tableParametersConfigOverride != null && !tableParametersConfigOverride.isEmpty()) {
      return tableParametersConfigOverride;
    }
    return tableParametersConfig.getParameters();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void tableReplicationStart(EventTableReplication tableReplication, String eventId) {
    tableParametersConfigOverride = Collections.emptyMap();
    Map<String, Object> transformOptions = tableReplication.getTransformOptions();
    Object tableParametersOverride = transformOptions.get(TableParametersConfig.TABLE_REPLICATION_OVERRIDE_TABLE_PARAMETERS);
    if (tableParametersOverride != null && tableParametersOverride instanceof Map) {
      tableParametersConfigOverride = (Map<String, String>) tableParametersOverride;
    }
  }

  @Override
  public void tableReplicationSuccess(EventTableReplication tableReplication, String eventId) {}

  @Override
  public void tableReplicationFailure(EventTableReplication tableReplication, String eventId, Throwable t) {}
}
