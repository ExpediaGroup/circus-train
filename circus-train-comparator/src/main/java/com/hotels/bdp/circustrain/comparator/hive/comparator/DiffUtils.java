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
package com.hotels.bdp.circustrain.comparator.hive.comparator;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import com.hotels.bdp.circustrain.api.CircusTrainTableParameter;
import com.hotels.bdp.circustrain.comparator.api.BaseDiff;
import com.hotels.bdp.circustrain.comparator.api.Diff;

public class DiffUtils {

  private DiffUtils() {}

  public static Diff<Object, Object> compareParameter(
      Map<String, String> parameters,
      CircusTrainTableParameter parameter,
      String expectedValue) {

    String parameterValue = parameters == null ? null : parameters.get(parameter.parameterName());

    if (parameterValue == null) {
      return new BaseDiff<Object, Object>("Replica parameter " + parameter.parameterName() + " not found",
          expectedValue, null);
    } else if (!parameterValue.equals(expectedValue)) {
      return new BaseDiff<Object, Object>("Replica parameter " + parameter.parameterName() + " is different",
          expectedValue, parameterValue);
    }

    return null;
  }

  public static Diff<Object, Object> compareParameter(
      Table replicaTable,
      CircusTrainTableParameter parameter,
      String expectedValue) {
    return compareParameter(replicaTable.getParameters(), parameter, expectedValue);
  }

  public static Diff<Object, Object> compareParameter(
      Partition replicaPartition,
      CircusTrainTableParameter parameter,
      String expectedValue) {
    return compareParameter(replicaPartition.getParameters(), parameter, expectedValue);
  }

}
