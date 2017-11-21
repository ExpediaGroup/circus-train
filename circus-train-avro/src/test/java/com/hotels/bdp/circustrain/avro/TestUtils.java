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
package com.hotels.bdp.circustrain.avro;

import java.util.HashMap;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

public class TestUtils {

  private TestUtils() {}

  public static Table newTable() {
    Table table = new Table();
    StorageDescriptor sd = new StorageDescriptor();
    SerDeInfo info = new SerDeInfo();
    info.setParameters(new HashMap<String, String>());
    sd.setSerdeInfo(info);
    table.setSd(sd);
    table.setParameters(new HashMap<String, String>());
    return table;
  }

  public static Partition newPartition() {
    Partition partition = new Partition();
    StorageDescriptor sd = new StorageDescriptor();
    SerDeInfo info = new SerDeInfo();
    info.setParameters(new HashMap<String, String>());
    sd.setSerdeInfo(info);
    partition.setSd(sd);
    partition.setParameters(new HashMap<String, String>());
    return partition;
  }
}
