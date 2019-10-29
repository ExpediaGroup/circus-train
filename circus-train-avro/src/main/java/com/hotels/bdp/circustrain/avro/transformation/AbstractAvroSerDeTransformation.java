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
package com.hotels.bdp.circustrain.avro.transformation;

import static com.hotels.bdp.circustrain.avro.conf.AvroSerDeConfig.AVRO_SERDE_OPTIONS;
import static com.hotels.bdp.circustrain.avro.conf.AvroSerDeConfig.BASE_URL;
import static com.hotels.bdp.circustrain.avro.util.AvroStringUtils.argsPresent;

import java.util.Collections;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.hotels.bdp.circustrain.api.conf.TransformOptions;
import com.hotels.bdp.circustrain.api.event.EventTableReplication;
import com.hotels.bdp.circustrain.api.event.TableReplicationListener;
import com.hotels.bdp.circustrain.avro.conf.AvroSerDeConfig;

public abstract class AbstractAvroSerDeTransformation implements TableReplicationListener {

  private final AvroSerDeConfig avroSerDeConfig = new AvroSerDeConfig();
  private String eventId;
  private String tableLocation;
  private Map<String, Object> avroSerdeConfigOverride = Collections.emptyMap();
  static final String AVRO_SCHEMA_URL_PARAMETER = "avro.schema.url";

  protected AbstractAvroSerDeTransformation(TransformOptions transformOptions) {
    if (transformOptions.getTransformOptions() == null) {
      return;
    }
    Object avroSerDeOptionsObj = transformOptions.getTransformOptions().get(AVRO_SERDE_OPTIONS);
    if (avroSerDeOptionsObj instanceof Map) {
      Map<String, String> avroSerDeOptionsMap = (Map<String, String>) avroSerDeOptionsObj;
      avroSerDeConfig.setBaseUrl(avroSerDeOptionsMap.get(BASE_URL));
    }
  }

  protected String getEventId() {
    return eventId;
  }

  protected String getTableLocation() {
    return tableLocation;
  }

  protected boolean avroTransformationSpecified() {
    return argsPresent(getAvroSchemaDestinationFolder(), getEventId());
  }

  protected String getAvroSchemaDestinationFolder() {
    Object urlOverride = avroSerdeConfigOverride.get(BASE_URL);
    if (urlOverride != null && StringUtils.isNotBlank(urlOverride.toString())) {
      return urlOverride.toString();
    } else if (avroSerDeConfig.getBaseUrl() != null && StringUtils.isNotBlank(avroSerDeConfig.getBaseUrl())) {
      return avroSerDeConfig.getBaseUrl();
    } else {
      return tableLocation;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void tableReplicationStart(EventTableReplication tableReplication, String eventId) {
    this.eventId = eventId;
    tableLocation = tableReplication.getReplicaTable().getTableLocation();
    avroSerdeConfigOverride = Collections.emptyMap();
    Map<String, Object> transformOptions = tableReplication.getTransformOptions();
    Object avroSerDeOverride = transformOptions.get(AVRO_SERDE_OPTIONS);
    if (avroSerDeOverride != null && avroSerDeOverride instanceof Map) {
      avroSerdeConfigOverride = (Map<String, Object>) avroSerDeOverride;
    }
  }

  @Override
  public void tableReplicationSuccess(EventTableReplication tableReplication, String eventId) {}

  @Override
  public void tableReplicationFailure(EventTableReplication tableReplication, String eventId, Throwable t) {}
}
