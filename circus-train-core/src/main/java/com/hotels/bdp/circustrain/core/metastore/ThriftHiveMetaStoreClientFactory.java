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
package com.hotels.bdp.circustrain.core.metastore;

import org.apache.hadoop.hive.conf.HiveConf;

import com.google.common.base.Strings;

import com.hotels.hcommon.hive.metastore.client.retrying.RetryingHiveMetaStoreClientFactory;

public class ThriftHiveMetaStoreClientFactory extends RetryingHiveMetaStoreClientFactory implements ConditionalMetaStoreClientFactory {

  public static final String ACCEPT_PREFIX = "thrift:";

  public ThriftHiveMetaStoreClientFactory(HiveConf hiveConf, String name) {
    super(hiveConf, name);
  }

  @Override
  public boolean accepts(String url) {
    return Strings.nullToEmpty(url).startsWith(ACCEPT_PREFIX);
  }
}
