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
package com.hotels.bdp.circustrain.comparator;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import com.hotels.bdp.circustrain.comparator.api.Comparator;
import com.hotels.bdp.circustrain.comparator.api.ComparatorType;
import com.hotels.bdp.circustrain.comparator.hive.comparator.FieldSchemaComparator;
import com.hotels.bdp.circustrain.comparator.hive.comparator.PartitionAndMetadataComparator;
import com.hotels.bdp.circustrain.comparator.hive.comparator.TableAndMetadataComparator;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.PartitionAndMetadata;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.TableAndMetadata;

public class ComparatorRegistry {

  private final ComparatorType comparatorType;
  private final Map<Class<?>, Comparator<?, ?>> registry = new HashMap<>();

  public ComparatorRegistry(ComparatorType comparatorType) {
    this.comparatorType = comparatorType;
    registry.put(FieldSchema.class, new FieldSchemaComparator(comparatorType));
    registry.put(PartitionAndMetadata.class, new PartitionAndMetadataComparator(this, comparatorType));
    registry.put(TableAndMetadata.class, new TableAndMetadataComparator(this, comparatorType));
  }

  public ComparatorType getComparatorType() {
    return comparatorType;
  }

  public Comparator<?, ?> comparatorFor(Class<?> type) {
    return registry.get(type);
  }

}
