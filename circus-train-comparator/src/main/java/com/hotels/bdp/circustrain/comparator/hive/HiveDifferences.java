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
package com.hotels.bdp.circustrain.comparator.hive;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.SOURCE_LOCATION;
import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.SOURCE_TABLE;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.CircusTrainTableParameter;
import com.hotels.bdp.circustrain.comparator.ComparatorRegistry;
import com.hotels.bdp.circustrain.comparator.api.Comparator;
import com.hotels.bdp.circustrain.comparator.api.Diff;
import com.hotels.bdp.circustrain.comparator.api.DiffListener;
import com.hotels.bdp.circustrain.comparator.hive.functions.CleanPartitionFunction;
import com.hotels.bdp.circustrain.comparator.hive.functions.CleanTableFunction;
import com.hotels.bdp.circustrain.comparator.hive.functions.PathDigest;
import com.hotels.bdp.circustrain.comparator.hive.functions.PathToPathMetadata;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.PartitionAndMetadata;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.TableAndMetadata;
import com.hotels.bdp.circustrain.hive.fetcher.PartitionFetcher;
import com.hotels.bdp.circustrain.hive.fetcher.PartitionNotFoundException;

public class HiveDifferences {

  private static final Function<TableAndMetadata, TableAndMetadata> CLEAN_TABLE_FUNCTION = new CleanTableFunction();
  private static final Function<PartitionAndMetadata, PartitionAndMetadata> CLEAN_PARTITION_FUNCTION = new CleanPartitionFunction();

  @VisibleForTesting
  static TableAndMetadata sourceTableToTableAndMetadata(Table sourceTable) {
    return new TableAndMetadata(Warehouse.getQualifiedName(sourceTable),
        normaliseLocation(sourceTable.getSd().getLocation()), sourceTable);
  }

  @VisibleForTesting
  static PartitionAndMetadata sourcePartitionToPartitionAndMetadata(Partition sourcePartition) {
    return new PartitionAndMetadata(sourcePartition.getDbName() + "." + sourcePartition.getTableName(),
        normaliseLocation(sourcePartition.getSd().getLocation()), sourcePartition);
  }

  @VisibleForTesting
  static TableAndMetadata replicaTableToTableAndMetadata(Table replicaTable) {
    return new TableAndMetadata(replicaTable.getParameters().get(SOURCE_TABLE.parameterName()),
        normaliseLocation(replicaTable.getParameters().get(SOURCE_LOCATION.parameterName())), replicaTable);
  }

  @VisibleForTesting
  static PartitionAndMetadata replicaPartitionToPartitionAndMetadata(Partition replicaPartition) {
    return new PartitionAndMetadata(replicaPartition.getParameters().get(SOURCE_TABLE.parameterName()),
        normaliseLocation(replicaPartition.getParameters().get(SOURCE_LOCATION.parameterName())), replicaPartition);
  }

  private static String normaliseLocation(String location) {
    if (location == null || location.endsWith("/")) {
      return location;
    }
    return location + "/";
  }

  public static class Builder {
    private final DiffListener diffListener;
    private ComparatorRegistry comparatorRegistry;
    private Configuration sourceConfiguration;
    private Table sourceTable;
    private Iterator<Partition> sourcePartitionIterator;
    private Optional<Table> replicaTable;
    private Optional<? extends PartitionFetcher> replicaPartitionFetcher;
    private Function<Path, String> checksumFunction;

    private Builder(DiffListener diffListener) {
      this.diffListener = diffListener;
    }

    public Builder comparatorRegistry(ComparatorRegistry comparatorRegistry) {
      this.comparatorRegistry = comparatorRegistry;
      return this;
    }

    public Builder source(
        Configuration sourceConfiguration,
        Table sourceTable,
        Iterator<Partition> sourcePartitionIterator) {
      this.sourceConfiguration = sourceConfiguration;
      this.sourceTable = sourceTable;
      this.sourcePartitionIterator = sourcePartitionIterator;
      return this;
    }

    public Builder replica(Optional<Table> replicaTable, Optional<? extends PartitionFetcher> replicaPartitionFetcher) {
      this.replicaTable = replicaTable;
      this.replicaPartitionFetcher = replicaPartitionFetcher;
      return this;
    }

    public Builder checksumFunction(Function<Path, String> checksumFunction) {
      this.checksumFunction = checksumFunction;
      return this;
    }

    public HiveDifferences build() {
      checkNotNull(diffListener, "diffListener is required");
      checkNotNull(comparatorRegistry, "comparatorRegistry is required");
      checkNotNull(sourceConfiguration, "sourceConfiguration is required");
      checkNotNull(sourceTable, "sourceTable is required");
      checkNotNull(sourcePartitionIterator, "sourcePartitionIterable is required");
      if (replicaTable.isPresent() && !replicaPartitionFetcher.isPresent()) {
        throw new IllegalStateException("replicaPartitionFetcher is required if replicaTable exists");
      }
      if (checksumFunction == null) {
        checksumFunction = Functions.compose(new PathDigest(), new PathToPathMetadata(sourceConfiguration));
      }
      return new HiveDifferences(comparatorRegistry, diffListener, sourceTable, sourcePartitionIterator, replicaTable,
          replicaPartitionFetcher, checksumFunction);
    }
  }

  public static Builder builder(DiffListener diffListener) {
    return new Builder(diffListener);
  }

  private final DiffListener diffListener;
  private final ComparatorRegistry comparatorRegistry;
  private final Table sourceTable;
  private final Iterator<Partition> sourcePartitionIterator;
  private final Optional<Table> replicaTable;
  private final Optional<? extends PartitionFetcher> replicaPartitionFetcher;
  private final Function<Path, String> checksumFunction;

  private HiveDifferences(
      ComparatorRegistry comparatorRegistry,
      DiffListener diffListener,
      Table sourceTable,
      Iterator<Partition> sourcePartitionIterator,
      Optional<Table> replicaTable,
      Optional<? extends PartitionFetcher> replicaPartitionFetcher,
      Function<Path, String> checksumFunction) {
    this.diffListener = diffListener;
    this.comparatorRegistry = comparatorRegistry;
    this.sourceTable = sourceTable;
    this.sourcePartitionIterator = sourcePartitionIterator;
    this.replicaTable = replicaTable;
    this.replicaPartitionFetcher = replicaPartitionFetcher;
    this.checksumFunction = checksumFunction;
  }

  @SuppressWarnings("unchecked")
  private <T> Comparator<T, Object> comparator(Class<T> clazz) {
    Comparator<T, Object> comparator = (Comparator<T, Object>) comparatorRegistry.comparatorFor(clazz);
    if (comparator == null) {
      throw new CircusTrainException("Unable to find a Comparator for class " + clazz.getName());
    }
    return comparator;
  }

  private static String partitionName(Table table, Partition partition) {
    try {
      return Warehouse.makePartName(table.getPartitionKeys(), partition.getValues());
    } catch (MetaException e) {
      throw new CircusTrainException("Unable to build partition name for partition values "
          + partition.getValues()
          + " of table "
          + Warehouse.getQualifiedName(table), e);
    }
  }

  public void run() {
    TableAndMetadata source = CLEAN_TABLE_FUNCTION.apply(sourceTableToTableAndMetadata(sourceTable));
    Optional<TableAndMetadata> replica = Optional.absent();
    if (replicaTable.isPresent()) {
      replica = Optional.of(CLEAN_TABLE_FUNCTION.apply(replicaTableToTableAndMetadata(replicaTable.get())));
    }
    diffListener.onDiffStart(source, replica);
    List<Diff<Object, Object>> tableDiffs = comparator(TableAndMetadata.class).compare(source, replica.orNull());
    if (!tableDiffs.isEmpty()) {
      diffListener.onChangedTable(tableDiffs);
    }

    while (sourcePartitionIterator.hasNext()) {
      Partition sourcePartition = sourcePartitionIterator.next();
      String sourcePartitionName = partitionName(source.getTable(), sourcePartition);
      PartitionAndMetadata sourcePartitionAndMetadata = CLEAN_PARTITION_FUNCTION
          .apply(sourcePartitionToPartitionAndMetadata(sourcePartition));

      Partition replicaPartition = null;
      try {
        if (replica.isPresent()) {
          replicaPartition = replicaPartitionFetcher.get().fetch(sourcePartitionName);
        }
      } catch (PartitionNotFoundException e) {
        // Ignore ...
      }
      if (replicaPartition == null) {
        diffListener.onNewPartition(sourcePartitionName, sourcePartition);
        continue;
      }

      PartitionAndMetadata replicaPartitionAndMetadata = CLEAN_PARTITION_FUNCTION
          .apply(replicaPartitionToPartitionAndMetadata(replicaPartition));
      List<Diff<Object, Object>> partitionDiffs = comparator(PartitionAndMetadata.class)
          .compare(sourcePartitionAndMetadata, replicaPartitionAndMetadata);
      if (!partitionDiffs.isEmpty()) {
        diffListener.onChangedPartition(sourcePartitionName, sourcePartition, partitionDiffs);
        continue;
      }

      String sourceChecksum = checksumFunction.apply(new Path(sourcePartitionAndMetadata.getSourceLocation()));
      String replicaChecksum = replicaPartition
          .getParameters()
          .get(CircusTrainTableParameter.PARTITION_CHECKSUM.parameterName());
      if (replicaChecksum == null || !sourceChecksum.equals(replicaChecksum)) {
        diffListener.onDataChanged(sourcePartitionName, sourcePartition);
      }

      // Partition remains unchanged
    }
    diffListener.onDiffEnd();
  }

}
