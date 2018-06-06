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
package com.hotels.bdp.circustrain.avro.conf;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "avro-serde-options")
public class AvroSerDeConfig {

  public static final String TABLE_REPLICATION_OVERRIDE_BASE_URL = "base-url";
  public static final String TABLE_REPLICATION_OVERRIDE_SOURCE_AVRO_URL_SCHEME = "source-avro-url-scheme";
  public static final String TABLE_REPLICATION_OVERRIDE_AVRO_SERDE_OPTIONS = "avro-serde-options";

  private String avroSchemaBaseUrl;
  private String sourceAvroUrlScheme;

  public String getBaseUrl() {
    return avroSchemaBaseUrl;
  }

  public void setBaseUrl(String avroSchemaBaseUrl) {
    this.avroSchemaBaseUrl = avroSchemaBaseUrl;
  }

  public String getSourceAvroUrlScheme() {
    return sourceAvroUrlScheme;
  }

  public void setSourceAvroUrlScheme(String sourceAvroUrlScheme) {
    this.sourceAvroUrlScheme = sourceAvroUrlScheme;
  }
}
