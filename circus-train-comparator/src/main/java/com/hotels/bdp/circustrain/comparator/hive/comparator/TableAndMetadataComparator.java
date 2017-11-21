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

import com.google.common.collect.ImmutableList;

import com.hotels.bdp.circustrain.comparator.ComparatorRegistry;
import com.hotels.bdp.circustrain.comparator.api.ComparatorType;
import com.hotels.bdp.circustrain.comparator.comparator.PropertyComparator;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.TableAndMetadata;

public class TableAndMetadataComparator extends PropertyComparator<TableAndMetadata> {

  public TableAndMetadataComparator(ComparatorRegistry comparatorRegistry, ComparatorType comparatorType) {
    super(comparatorRegistry, comparatorType,
        ImmutableList.of("sourceTable", "sourceLocation", "table.parameters", "table.partitionKeys", "table.retention",
            "table.sd.inputFormat", "table.sd.outputFormat", "table.sd.parameters", "table.sd.serdeInfo",
            "table.sd.skewedInfo", "table.sd.cols", "table.sd.sortCols", "table.sd.bucketCols", "table.sd.numBuckets"));
  }

}
