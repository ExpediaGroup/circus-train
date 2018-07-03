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
package com.hotels.bdp.circustrain.api.conf;

import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.openpojo.reflection.PojoClass;
import com.openpojo.reflection.impl.PojoClassFactory;
import com.openpojo.validation.ValidatorBuilder;
import com.openpojo.validation.rule.impl.GetterMustExistRule;
import com.openpojo.validation.rule.impl.SetterMustExistRule;
import com.openpojo.validation.test.impl.GetterTester;
import com.openpojo.validation.test.impl.SetterTester;

import com.hotels.bdp.circustrain.api.conf.MetastoreTunnel;
import com.hotels.bdp.circustrain.api.conf.MetricsReporter;
import com.hotels.bdp.circustrain.api.conf.ReplicaCatalog;
import com.hotels.bdp.circustrain.api.conf.ReplicaTable;
import com.hotels.bdp.circustrain.api.conf.Security;
import com.hotels.bdp.circustrain.api.conf.SourceCatalog;
import com.hotels.bdp.circustrain.api.conf.SourceTable;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.conf.TableReplications;

public class ConfigurationPojosTest {

  @Test
  public void testPojo() {
    ValidatorBuilder
        .create()
        .with(new GetterMustExistRule())
        .with(new SetterMustExistRule())
        .with(new SetterTester())
        .with(new GetterTester())
        .build()
        .validate(Lists.transform(
            ImmutableList
                .<Class<?>> builder()
                .add(MetastoreTunnel.class)
                .add(ReplicaTable.class)
                .add(ReplicaCatalog.class)
                .add(Security.class)
                .add(SourceCatalog.class)
                .add(SourceTable.class)
                .add(TableReplication.class)
                .add(TableReplications.class)
                .add(MetricsReporter.class)
                .build(),
            new Function<Class<?>, PojoClass>() {
              @Override
              public PojoClass apply(Class<?> input) {
                return PojoClassFactory.getPojoClass(input);
              }
            }));
  }

}
