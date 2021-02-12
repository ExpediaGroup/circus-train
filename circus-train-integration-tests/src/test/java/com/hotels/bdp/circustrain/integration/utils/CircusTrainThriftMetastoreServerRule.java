/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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
package com.hotels.bdp.circustrain.integration.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;

public class CircusTrainThriftMetastoreServerRule extends ThriftHiveMetaStoreJUnitRule {
  
  private static Map<String, String> createConfigOverride(HiveConf hiveConf) {
    Map<String, String> overrideConfig = new HashMap<String, String>();
    // Override with values given in the hiveConf.
    overrideConfig.put(ConfVars.METASTORECONNECTURLKEY.toString(), hiveConf.getVar(ConfVars.METASTORECONNECTURLKEY));
    overrideConfig.put(ConfVars.METASTORE_CONNECTION_DRIVER.toString(), hiveConf.getVar(ConfVars.METASTORE_CONNECTION_DRIVER));
    overrideConfig.put(ConfVars.METASTORE_CONNECTION_USER_NAME.toString(), hiveConf.getVar(ConfVars.METASTORE_CONNECTION_USER_NAME));
    overrideConfig.put(ConfVars.METASTOREPWD.toString(), hiveConf.getVar(ConfVars.METASTOREPWD));
    return overrideConfig;
  }

  public CircusTrainThriftMetastoreServerRule(HiveConf hiveConf) {
    super("test_database", Collections.emptyMap(), createConfigOverride(hiveConf));
  }

}
