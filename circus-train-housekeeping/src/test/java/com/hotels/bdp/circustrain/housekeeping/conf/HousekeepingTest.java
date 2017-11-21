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

import org.junit.BeforeClass;
import org.junit.Test;

public class HousekeepingTest {

  private static Validator validator;

  private final Housekeeping housekeeping = new Housekeeping();

  @BeforeClass
  public static void setUp() {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    validator = factory.getValidator();
  }

  @Test
  public void typical() {
    Set<ConstraintViolation<Housekeeping>> constraintViolations = validator.validate(housekeeping);
    assertThat(constraintViolations.size(), is(0));
  }

  @Test
  public void missingExpiredPathDuration() {
    housekeeping.setExpiredPathDuration(null);
    Set<ConstraintViolation<Housekeeping>> constraintViolations = validator.validate(housekeeping);
    assertThat(constraintViolations.size(), is(1));
    assertThat(constraintViolations.iterator().next().getMessage(),
        is("housekeeping.expiredPathDuration must not be null"));
  }

  @Test
  public void missingDataSource() {
    housekeeping.setDataSource(null);
    Set<ConstraintViolation<Housekeeping>> constraintViolations = validator.validate(housekeeping);
    assertThat(constraintViolations.size(), is(1));
    assertThat(constraintViolations.iterator().next().getMessage(), is("housekeeping.dataSource must not be null"));
  }

}
