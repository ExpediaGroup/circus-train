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
package com.hotels.bdp.circustrain.core.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;
import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClientFactory;
import com.hotels.bdp.circustrain.api.metastore.MetaStoreClientException;
import com.hotels.bdp.circustrain.api.metastore.MetaStoreClientFactory;

public class ThriftMetaStoreClientFactory implements MetaStoreClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftMetaStoreClientFactory.class);

  public static final String ACCEPT_PREFIX = "thrift:";

  @Override
  public CloseableMetaStoreClient newInstance(HiveConf conf, String name) {
    LOG.debug("Connecting to '{}' metastore at '{}'", name, conf.getVar(ConfVars.METASTOREURIS));
    try {
      return CloseableMetaStoreClientFactory
          .newInstance(RetryingMetaStoreClient.getProxy(conf, new HiveMetaHookLoader() {
            @Override
            public HiveMetaHook getHook(Table tbl) throws MetaException {
              return null;
            }
          }, HiveMetaStoreClient.class.getName()));
    } catch (MetaException | RuntimeException e) {
      String message = String.format("Unable to connect to '%s' metastore at '%s'", name,
          conf.getVar(ConfVars.METASTOREURIS));
      throw new MetaStoreClientException(message, e);
    }
  }

  @Override
  public boolean accepts(String url) {
    return Strings.nullToEmpty(url).startsWith(ACCEPT_PREFIX);
  }
}
