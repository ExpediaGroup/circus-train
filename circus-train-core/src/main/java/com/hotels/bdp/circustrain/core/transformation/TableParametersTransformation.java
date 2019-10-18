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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.conf.TransformOptions;
import com.hotels.bdp.circustrain.api.metadata.TableTransformation;

@Profile({ Modules.REPLICATION })
@Component
public final class TableParametersTransformation extends AbstractTableParametersTransformation implements TableTransformation {

  @Autowired
  public TableParametersTransformation(TransformOptions transformOptions) {
    super(transformOptions);
  }

  @Override
  public Table transform(Table table) {
    Map<String, String> tableParameters = getTableParameters();
    if (tableParameters == null || tableParameters.isEmpty()) {
      return table;
    }
    tableParameters = mergeTableParameters(tableParameters, table);
    table.setParameters(tableParameters);
    return table;
  }

  private Map<String, String> mergeTableParameters(Map<String, String> tableParameters, Table table) {
    Map<String, String> parameters;
    if (table.getParameters() != null) {
      parameters = new LinkedHashMap<>(table.getParameters());
    } else {
      parameters = new LinkedHashMap<>();
    }
    parameters.putAll(tableParameters);
    return parameters;
  }

}
