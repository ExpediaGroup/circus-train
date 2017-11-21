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
package com.hotels.bdp.circustrain.s3mapreducecp.jcommander;

import org.junit.Test;

import com.beust.jcommander.ParameterException;

public class PositiveNonZeroIntegerTest {

  private final PositiveNonZeroInteger validator = new PositiveNonZeroInteger();

  @Test
  public void typical() {
    validator.validate("var", "123");
  }

  @Test(expected = ParameterException.class)
  public void zero() {
    validator.validate("var", "0");
  }

  @Test(expected = ParameterException.class)
  public void negative() {
    validator.validate("var", "-1");
  }

  @Test(expected = NumberFormatException.class)
  public void invalidFormat() {
    validator.validate("var", "abc");
  }

}
