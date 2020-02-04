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
package com.hotels.bdp.circustrain.comparator.listener;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import com.hotels.bdp.circustrain.comparator.api.Diff;
import com.hotels.bdp.circustrain.comparator.api.DiffListener;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.TableAndMetadata;

public class PartitionSpecCreatingDiffListener implements DiffListener {

  private final static Logger LOG = LoggerFactory.getLogger(PartitionSpecCreatingDiffListener.class);

  private static final String HIVE_STRING_TYPE = "string";

  private final Set<List<String>> partitionValues = new LinkedHashSet<>();
  private List<FieldSchema> partitionKeys;

  private final String hiveDefaultPartitionName;

  public PartitionSpecCreatingDiffListener(Configuration conf) {
    // https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties
    hiveDefaultPartitionName = conf.get("hive.exec.default.partition.name", "__HIVE_DEFAULT_PARTITION__");
  }

  @Override
  public void onDiffStart(TableAndMetadata source, Optional<TableAndMetadata> replica) {
    partitionKeys = source.getTable().getPartitionKeys();
  }

  @Override
  public void onChangedTable(List<Diff<Object, Object>> differences) {
    // Table changes are handled by CT
  }

  @Override
  public void onNewPartition(String partitionName, Partition partition) {
    addPartition(partition);
  }

  @Override
  public void onChangedPartition(String partitionName, Partition partition, List<Diff<Object, Object>> differences) {
    addPartition(partition);
  }

  @Override
  public void onDataChanged(String partitionName, Partition partition) {
    addPartition(partition);
  }

  @Override
  public void onDiffEnd() {}

  private void addPartition(Partition partition) {
    partitionValues.add(partition.getValues());
  }

  public String getPartitionSpecFilter() {
    LOG.info("Creating partition spec from '{}' detected partitions.", partitionValues.size());
    List<String> filterPartitions = new ArrayList<>();
    for (List<String> values : partitionValues) {
      if (!values.contains(hiveDefaultPartitionName)) {
        filterPartitions.add("(" + Joiner.on(" AND ").withKeyValueSeparator("=").join(quoteValues(values)) + ")");
      } else {
        LOG.warn("Can't replicate partition with these values {}, will skip them.", values);
      }
    }
    return Joiner.on(" OR ").join(filterPartitions);
  }

  private Map<String, String> quoteValues(List<String> values) {
    Map<String, String> result = new LinkedHashMap<>(partitionKeys.size());
    for (int i = 0; i < partitionKeys.size(); i++) {
      FieldSchema field = partitionKeys.get(i);
      String value = values.get(i);
      if (HIVE_STRING_TYPE.equalsIgnoreCase(field.getType())) {
        result.put(field.getName(), "'" + value + "'");
      } else {
        result.put(field.getName(), value);
      }
    }
    return result;
  }

}
