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

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.conf.TableReplication;
import com.hotels.bdp.circustrain.core.conf.SpringExpressionParser;

public class SpelParsedPartitionPredicate implements PartitionPredicate {

  private final static Logger LOG = LoggerFactory.getLogger(SpelParsedPartitionPredicate.class);

  private final TableReplication tableReplication;
  private final SpringExpressionParser expressionParser;

  public SpelParsedPartitionPredicate(SpringExpressionParser expressionParser, TableReplication tableReplication) {
    this.expressionParser = expressionParser;
    this.tableReplication = tableReplication;
  }

  @Override
  public String getPartitionPredicate() {
    String partitionFilter = tableReplication.getSourceTable().getPartitionFilter();
    String parsedPartitionFilter = expressionParser.parse(partitionFilter);
    if (!Objects.equals(partitionFilter, parsedPartitionFilter)) {
      LOG.info("Parsed partitionFilter: " + parsedPartitionFilter);
    }
    return parsedPartitionFilter;
  }

  @Override
  public short getPartitionPredicateLimit() {
    Short partitionLimit = tableReplication.getSourceTable().getPartitionLimit();
    return partitionLimit == null ? -1 : partitionLimit;
  }
}
