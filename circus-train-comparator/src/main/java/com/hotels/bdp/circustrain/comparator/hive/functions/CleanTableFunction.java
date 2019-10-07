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
package com.hotels.bdp.circustrain.comparator.hive.functions;

import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.base.Function;

import com.hotels.bdp.circustrain.api.CircusTrainTableParameter;
import com.hotels.bdp.circustrain.comparator.api.ExcludedHiveParameter;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.TableAndMetadata;

public class CleanTableFunction implements Function<TableAndMetadata, TableAndMetadata> {

  @Override
  public TableAndMetadata apply(TableAndMetadata tableAndMetadata) {
    if (tableAndMetadata == null) {
      return null;
    }

    Table tableCopy = tableAndMetadata.getTable().deepCopy();
    for (CircusTrainTableParameter p : CircusTrainTableParameter.values()) {
      tableCopy.getParameters().remove(p.parameterName());
    }
    for (ExcludedHiveParameter p : ExcludedHiveParameter.values()) {
      tableCopy.getParameters().remove(p.parameterName());
    }
    return new TableAndMetadata(tableAndMetadata.getSourceTable(), tableAndMetadata.getSourceLocation(), tableCopy);
  }

}
