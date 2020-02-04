/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.hotels.bdp.circustrain.core.transformation;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Partition;

import com.hotels.bdp.circustrain.api.metadata.PartitionTransformation;

public class CompositePartitionTransformation implements PartitionTransformation {

  private final List<PartitionTransformation> partitionTransformations;

  public CompositePartitionTransformation(List<PartitionTransformation> partitionTransformations) {
    this.partitionTransformations = Collections.unmodifiableList(partitionTransformations);
  }

  @Override
  public Partition transform(Partition partition) {
    Partition transformedPartition = partition;
    for (PartitionTransformation partitionTransformation : partitionTransformations) {
      transformedPartition = partitionTransformation.transform(transformedPartition);
    }
    return transformedPartition;
  }
}
