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
package com.hotels.bdp.circustrain.hive.iterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Works out the splits needed to chunk a set of partitions into batches. */
class BatchResolver {

  private static final Logger LOG = LoggerFactory.getLogger(BatchResolver.class);

  private final List<String> names;
  private final short batchSize;

  BatchResolver(List<String> names, short batchSize) {
    this.names = names;
    this.batchSize = batchSize;
  }

  List<List<String>> resolve() throws MetaException, TException {
    if (names.size() <= batchSize) {
      LOG.debug("Number of partitions ({}) is less than batch size ({}).", names.size(), batchSize);
      return Collections.singletonList(names);
    }

    List<List<String>> partitionValueBounds = new ArrayList<>(names.size() / batchSize + 1);
    int n = names.size();
    for (int i = 0; i < names.size(); i += batchSize) {
      partitionValueBounds.add(names.subList(i, Math.min(n, i + batchSize)));
    }
    return partitionValueBounds;
  }

}
