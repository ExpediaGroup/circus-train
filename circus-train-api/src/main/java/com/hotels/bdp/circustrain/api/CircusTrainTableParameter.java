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
package com.hotels.bdp.circustrain.api;

public enum CircusTrainTableParameter {
  SOURCE_TABLE("com.hotels.bdp.circustrain.source.table"),
  SOURCE_METASTORE("com.hotels.bdp.circustrain.source.metastore.uris"),
  SOURCE_LOCATION("com.hotels.bdp.circustrain.source.location"),
  REPLICATION_EVENT("com.hotels.bdp.circustrain.replication.event"),
  LAST_REPLICATED("com.hotels.bdp.circustrain.last.replicated"),
  PARTITION_CHECKSUM("com.hotels.bdp.circustrain.partition.checksum"),
  REPLICATION_MODE("com.hotels.bdp.circustrain.replication.mode");

  private final String parameterName;

  private CircusTrainTableParameter(String parameterName) {
    this.parameterName = parameterName;
  }

  public String parameterName() {
    return parameterName;
  }

}
