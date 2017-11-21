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
package com.hotels.bdp.circustrain.validation.constraintvalidators;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import javax.validation.ConstraintValidatorContext;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class URIValidatorTest {

  private @Mock com.hotels.bdp.circustrain.validation.constraints.URI uriConstraint;
  private @Mock ConstraintValidatorContext constraintValidatorContext;

  private final URIValidator validator = new URIValidator();

  @Before
  public void init() {
    when(uriConstraint.scheme()).thenReturn("");
    when(uriConstraint.host()).thenReturn("");
    when(uriConstraint.port()).thenReturn(-1);
    validator.initialize(uriConstraint);
  }

  @Test
  public void validNullUri() {
    assertTrue(validator.isValid(null, constraintValidatorContext));
  }

  @Test
  public void validEmptyUri() {
    assertTrue(validator.isValid("", constraintValidatorContext));
  }

  @Test
  public void validUri() {
    assertTrue(validator.isValid("http://localhost:80/index.html", constraintValidatorContext));
  }

  @Test
  public void invalid() {
    assertFalse(validator.isValid("//", constraintValidatorContext));
  }

  @Test
  public void notMatchingScheme() {
    when(uriConstraint.scheme()).thenReturn("jdbc");
    validator.initialize(uriConstraint);
    assertFalse(validator.isValid("jmx:imd:/foo/bar", constraintValidatorContext));
  }

  @Test
  public void notMatchingHost() {
    when(uriConstraint.host()).thenReturn("hotels.com");
    validator.initialize(uriConstraint);
    assertFalse(validator.isValid("http://localhost:80/index.html", constraintValidatorContext));
  }

  @Test
  public void notMatchingPort() {
    when(uriConstraint.port()).thenReturn(8080);
    validator.initialize(uriConstraint);
    assertFalse(validator.isValid("http://localhost:80/index.html", constraintValidatorContext));
  }

}
