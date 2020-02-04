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
package com.hotels.bdp.circustrain.tool.filter;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.ParseException;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.conf.SourceTable;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.core.HiveEndpointFactory;
import com.hotels.bdp.circustrain.core.PartitionPredicate;
import com.hotels.bdp.circustrain.core.PartitionPredicateFactory;
import com.hotels.bdp.circustrain.core.TableAndStatistics;
import com.hotels.bdp.circustrain.core.conf.SpringExpressionParser;
import com.hotels.bdp.circustrain.tool.core.endpoint.ReplicaHiveEndpoint;
import com.hotels.bdp.circustrain.tool.core.endpoint.SourceHiveEndpoint;

class FilterGeneratorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(FilterGeneratorFactory.class);

  private final static FilterGenerator DUMMY_FILTER_GENERATOR = new FilterGenerator() {
    @Override
    public void run() throws CircusTrainException {}
  };

  private final HiveEndpointFactory<SourceHiveEndpoint> sourceFactory;
  private final HiveEndpointFactory<ReplicaHiveEndpoint> replicaFactory;
  private final SpringExpressionParser expressionParser;

  FilterGeneratorFactory(
      HiveEndpointFactory<SourceHiveEndpoint> sourceFactory,
      HiveEndpointFactory<ReplicaHiveEndpoint> replicaFactory,
      SpringExpressionParser expressionParser) {
    this.sourceFactory = sourceFactory;
    this.replicaFactory = replicaFactory;
    this.expressionParser = expressionParser;
  }

  public FilterGenerator newInstance(TableReplication tableReplication) {
    SourceTable sourceTable = tableReplication.getSourceTable();
    String sourceDatabaseName = sourceTable.getDatabaseName();
    String sourceTableName = sourceTable.getTableName();
    String partitionFilter = sourceTable.getPartitionFilter();

    SourceHiveEndpoint source = sourceFactory.newInstance(tableReplication);
    TableAndStatistics tableAndStatistics = source.getTableAndStatistics(sourceDatabaseName, sourceTableName);
    List<FieldSchema> partitionKeys = tableAndStatistics.getTable().getPartitionKeys();
    if (partitionKeys != null && !partitionKeys.isEmpty()) {
      if (!tableReplication.getSourceTable().isGeneratePartitionFilter()) {
        checkSpelFilter(partitionFilter);
      }
      PartitionPredicate partitionPredicate = new PartitionPredicateFactory(sourceFactory, replicaFactory,
          expressionParser, null).newInstance(tableReplication);
      return new FilterGeneratorImpl(source, tableAndStatistics.getTable(), partitionFilter, partitionPredicate);
    } else {
      return DUMMY_FILTER_GENERATOR;
    }
  }

  /**
   * check filter and generate more friendly message than CT normally does
   *
   * @param partitionFilter
   */
  private void checkSpelFilter(String partitionFilter) {
    LOG.info("Located partition filter expression: {}", partitionFilter);
    try {
      expressionParser.parse(partitionFilter);
    } catch (ParseException e) {
      System.out.println("WARNING: There was a problem parsing your expression. Check above to see what was "
          + "resolved from the YML file to see if it is what you expect.");
      System.out.println("Perhaps you are inadvertently creating a YAML comment with '#{ #'?");
      e.printStackTrace(System.out);
      return;
    }
  }
}
