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
package com.hotels.bdp.circustrain.s3mapreducecp;

import org.apache.hadoop.conf.Configuration;

public class S3MapReduceCpConfiguration extends Configuration {

  public S3MapReduceCpConfiguration() {}

  public S3MapReduceCpConfiguration(Configuration configuration) {
    super(configuration);
  }

  public String get(ConfigurationVariable var) {
    return get(var.getName(), var.defaultValue());
  }

  public void set(ConfigurationVariable var, String value) {
    set(var.getName(), value);
  }

  public int getInt(ConfigurationVariable var) {
    return getInt(var.getName(), var.defaultIntValue());
  }

  public void setInt(ConfigurationVariable var, int value) {
    setInt(var.getName(), value);
  }

  public long getLong(ConfigurationVariable var) {
    return getLong(var.getName(), var.defaultLongValue());
  }

  public void setLong(ConfigurationVariable var, long value) {
    setLong(var.getName(), value);
  }

  public boolean getBoolean(ConfigurationVariable var) {
    return getBoolean(var.getName(), var.defaultBooleanValue());
  }

  public void setBoolean(ConfigurationVariable var, boolean value) {
    setBoolean(var.getName(), value);
  }

}
