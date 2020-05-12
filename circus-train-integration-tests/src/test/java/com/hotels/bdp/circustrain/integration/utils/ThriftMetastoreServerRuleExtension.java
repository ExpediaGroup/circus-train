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
package com.hotels.bdp.circustrain.integration.utils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;

public class ThriftMetastoreServerRuleExtension extends ThriftHiveMetaStoreJUnitRule {
  private final HiveConf hiveConf;

  public ThriftMetastoreServerRuleExtension(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  @Override
  public void beforeTest() throws Throwable {
    // Override with values given in the hiveConf.
    conf().setVar(ConfVars.METASTORECONNECTURLKEY, hiveConf.getVar(ConfVars.METASTORECONNECTURLKEY));
    conf().setVar(ConfVars.METASTORE_CONNECTION_DRIVER, hiveConf.getVar(ConfVars.METASTORE_CONNECTION_DRIVER));
    conf().setVar(ConfVars.METASTORE_CONNECTION_USER_NAME, hiveConf.getVar(ConfVars.METASTORE_CONNECTION_USER_NAME));
    conf().setVar(ConfVars.METASTOREPWD, hiveConf.getVar(ConfVars.METASTOREPWD));
    super.beforeTest();
  }

  @Override
  protected void afterTest() {
    super.afterTest();
  }

}
