/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
package com.hotels.bdp.circustrain.core;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Function;

import com.hotels.bdp.circustrain.conf.TableReplication;
import com.hotels.bdp.circustrain.core.conf.SpringExpressionParser;

public class PartitionPredicateFactory {

  private final SpringExpressionParser expressionParser;
  private final HiveEndpointFactory<? extends HiveEndpoint> sourceFactory;
  private final HiveEndpointFactory<? extends HiveEndpoint> replicaFactory;
  private final Function<Path, String> checksumFunction;

  public PartitionPredicateFactory(
      HiveEndpointFactory<? extends HiveEndpoint> sourceFactory,
      HiveEndpointFactory<? extends HiveEndpoint> replicaFactory,
      SpringExpressionParser expressionParser,
      Function<Path, String> checksumFunction) {
    this.sourceFactory = sourceFactory;
    this.replicaFactory = replicaFactory;
    this.expressionParser = expressionParser;
    this.checksumFunction = checksumFunction;
  }

  public PartitionPredicate newInstance(TableReplication tableReplication) {
    if (tableReplication.getSourceTable().isGeneratePartitionFilter()) {
      return new DiffGeneratedPartitionPredicate(sourceFactory.newInstance(tableReplication),
          replicaFactory.newInstance(tableReplication), tableReplication, checksumFunction);
    } else {
      return new SpelParsedPartitionPredicate(expressionParser, tableReplication);
    }
  }

}
