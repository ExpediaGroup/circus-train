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
package com.hotels.bdp.circustrain.core.conf;

import static com.hotels.bdp.circustrain.core.conf.CircusTrainTransformOptions.TRANSFORM_OPTIONS_BEAN;
import static com.hotels.bdp.circustrain.core.conf.CircusTrainTransformOptions.TRANSFORM_OPTIONS_PROPERTY;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.hotels.bdp.circustrain.api.conf.TransformOptions;

@Configuration(TRANSFORM_OPTIONS_BEAN)
@ConfigurationProperties(TRANSFORM_OPTIONS_PROPERTY)
public class CircusTrainTransformOptions extends TransformOptions {

  public static final String TRANSFORM_OPTIONS_BEAN = "transformOptions";
  public static final String TRANSFORM_OPTIONS_PROPERTY = "transform-options";
  public static final String TABLE_PROPERTIES = "table-properties";

}
