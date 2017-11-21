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

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import com.hotels.bdp.circustrain.comparator.api.Comparator;
import com.hotels.bdp.circustrain.comparator.api.ComparatorType;
import com.hotels.bdp.circustrain.comparator.hive.comparator.FieldSchemaComparator;
import com.hotels.bdp.circustrain.comparator.hive.comparator.PartitionAndMetadataComparator;
import com.hotels.bdp.circustrain.comparator.hive.comparator.TableAndMetadataComparator;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.PartitionAndMetadata;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.TableAndMetadata;

public class ComparatorRegistryTest {

  private final ComparatorRegistry comparatorRegistry = new ComparatorRegistry(ComparatorType.SHORT_CIRCUIT);

  @Test
  public void typical() {
    Comparator<?, ?> objectComparator = comparatorRegistry.comparatorFor(FieldSchema.class);
    assertThat(objectComparator, is(instanceOf(FieldSchemaComparator.class)));

    Comparator<?, ?> integerComparator = comparatorRegistry.comparatorFor(PartitionAndMetadata.class);
    assertThat(integerComparator, is(instanceOf(PartitionAndMetadataComparator.class)));

    Comparator<?, ?> stringComparator = comparatorRegistry.comparatorFor(TableAndMetadata.class);
    assertThat(stringComparator, is(instanceOf(TableAndMetadataComparator.class)));

    assertThat(comparatorRegistry.comparatorFor(Table.class), is(nullValue()));
    assertThat(comparatorRegistry.comparatorFor(Partition.class), is(nullValue()));
  }

}
