/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.hotels.bdp.circustrain.api.event;

import java.util.Map;

public class EventTableReplication {

  private final EventSourceTable sourceTable;
  private final EventReplicaTable replicaTable;
  private final Map<String, Object> copierOptions;
  private final String qualifiedName;
  private final Map<String, Object> transformOptions;

  public EventTableReplication(
      EventSourceTable sourceTable,
      EventReplicaTable replicaTable,
      Map<String, Object> copierOptions,
      String qualifiedName,
      Map<String, Object> transformOptions) {
    this.sourceTable = sourceTable;
    this.replicaTable = replicaTable;
    this.copierOptions = copierOptions;
    this.qualifiedName = qualifiedName;
    this.transformOptions = transformOptions;
  }

  public EventSourceTable getSourceTable() {
    return sourceTable;
  }

  public EventReplicaTable getReplicaTable() {
    return replicaTable;
  }

  public Map<String, Object> getCopierOptions() {
    return copierOptions;
  }

  public String getQualifiedReplicaName() {
    return qualifiedName;
  }

  public Map<String, Object> getTransformOptions() {
    return transformOptions;
  }

}
