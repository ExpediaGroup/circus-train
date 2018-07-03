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
package com.hotels.bdp.circustrain.validation.constraintvalidators;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import org.apache.commons.lang3.StringUtils;

import com.hotels.bdp.circustrain.conf.ReplicationMode;
import com.hotels.bdp.circustrain.conf.TableReplication;
import com.hotels.bdp.circustrain.validation.constraints.TableReplicationFullReplicationModeConstraint;

public class TableReplicationFullReplicationModeValidator
    implements ConstraintValidator<TableReplicationFullReplicationModeConstraint, TableReplication> {

  @Override
  public void initialize(TableReplicationFullReplicationModeConstraint constraintAnnotation) {

  }

  @Override
  public boolean isValid(TableReplication value, ConstraintValidatorContext context) {
    if (value.getReplicationMode() == ReplicationMode.FULL) {
      return StringUtils.isNotBlank(value.getReplicaTable().getTableLocation());
    }
    return true;
  }

}
