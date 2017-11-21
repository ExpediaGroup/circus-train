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
package com.hotels.bdp.circustrain.tool.comparison;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.hotels.bdp.circustrain.core.conf.TableReplication;
import com.hotels.bdp.circustrain.core.conf.TableReplications;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
class ComparisonApplication implements ApplicationRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ComparisonApplication.class);

  private final List<TableReplication> tableReplications;
  private final TableComparatorFactory tableComparisonFactory;

  @Autowired
  ComparisonApplication(TableReplications tableReplications, TableComparatorFactory tableComparisonFactory) {
    this.tableReplications = tableReplications.getTableReplications();
    this.tableComparisonFactory = tableComparisonFactory;
  }

  @Override
  public void run(ApplicationArguments args) {
    LOG.info("{} tables to compare.", tableReplications.size());
    for (TableReplication tableReplication : tableReplications) {
      try {
        TableComparator tableComparison = tableComparisonFactory.newInstance(tableReplication);
        tableComparison.run();
      } catch (Throwable t) {
        LOG.error("Failed.", t);
      }
    }

  }

}
