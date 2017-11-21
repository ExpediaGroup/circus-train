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
package com.hotels.bdp.circustrain.housekeeping.conf;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataSourceTest {

  private static Validator validator;

  private final DataSource dataSource = new DataSource();

  @BeforeClass
  public static void setUp() {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    validator = factory.getValidator();
  }

  @Before
  public void init() {
    dataSource
        .setUrl("jdbc:h2:/opt/circus-train/data/circus-train/housekeeping;AUTO_SERVER=TRUE;DB_CLOSE_ON_EXIT=FALSE");
  }

  @Test
  public void typical() {
    Set<ConstraintViolation<DataSource>> constraintViolations = validator.validate(dataSource);
    assertThat(constraintViolations.size(), is(0));
  }

  @Test
  public void missingDriverClassName() {
    dataSource.setDriverClassName(null);
    Set<ConstraintViolation<DataSource>> constraintViolations = validator.validate(dataSource);
    assertThat(constraintViolations.size(), is(1));
    assertThat(constraintViolations.iterator().next().getMessage(),
        is("housekeeping.dataSource.driverClassName must not be blank"));
  }

  @Test
  public void missingUrl() {
    dataSource.setUrl(null);
    Set<ConstraintViolation<DataSource>> constraintViolations = validator.validate(dataSource);
    assertThat(constraintViolations.size(), is(0));
  }

  @Test
  public void invalidUrl() {
    dataSource.setUrl("abc@123/09");
    Set<ConstraintViolation<DataSource>> constraintViolations = validator.validate(dataSource);
    assertThat(constraintViolations.size(), is(1));
    assertThat(constraintViolations.iterator().next().getMessage(),
        is("housekeeping.dataSource.url does not seem to be a valid JDBC URI"));
  }
}
