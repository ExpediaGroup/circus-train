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
package com.hotels.bdp.circustrain.comparator.api;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.Partition;

import com.google.common.base.Optional;

import com.hotels.bdp.circustrain.comparator.hive.wrappers.TableAndMetadata;

public interface DiffListener {

  void onDiffStart(TableAndMetadata source, Optional<TableAndMetadata> replica);

  void onDiffEnd();

  void onChangedTable(List<Diff<Object, Object>> differences);

  void onNewPartition(String partitionName, Partition partition);

  void onChangedPartition(String partitionName, Partition partition, List<Diff<Object, Object>> differences);

  void onDataChanged(String partitionName, Partition partition);

}
