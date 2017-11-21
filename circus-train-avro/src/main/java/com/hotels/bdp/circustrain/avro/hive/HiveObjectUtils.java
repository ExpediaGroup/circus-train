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
package com.hotels.bdp.circustrain.avro.hive;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

public final class HiveObjectUtils {

  private HiveObjectUtils() {}

  public static String getParameter(Table table, String parameter) {
    return getParameter(table.getParameters(), table.getSd().getSerdeInfo().getParameters(), parameter);
  }

  public static String getParameter(Partition partition, String parameter) {
    return getParameter(partition.getParameters(), partition.getSd().getSerdeInfo().getParameters(), parameter);
  }

  private static String getParameter(
      Map<String, String> tableProperties,
      Map<String, String> serdeProperties,
      String parameter) {
    return tableProperties.get(parameter) != null ? tableProperties.get(parameter) : serdeProperties.get(parameter);
  }

  public static Table updateSerDeUrl(Table table, String parameter, String url) {
    updateSerDeUrl(table.getParameters(), table.getSd().getSerdeInfo().getParameters(), parameter, url);
    return table;
  }

  public static Partition updateSerDeUrl(Partition partition, String parameter, String url) {
    updateSerDeUrl(partition.getParameters(), partition.getSd().getSerdeInfo().getParameters(), parameter, url);
    return partition;
  }

  private static void updateSerDeUrl(
      Map<String, String> tableProperties,
      Map<String, String> serdeProperties,
      String parameter,
      String url) {
    tableProperties.put(parameter, url);
    serdeProperties.remove(parameter);
  }
}
