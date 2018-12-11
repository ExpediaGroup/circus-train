/**
 * Copyright (C) 2016-2017 Expedia Inc and JCommander contributors.
 *
 * Based on {@code com.beust.jcommander.validators.PostivieInteger} from JCommander 1.48:
 *
 * https://github.com/cbeust/jcommander/blob/jcommander-1.48/src/main/java/com/beust/
 * jcommander/validators/PositiveInteger.java
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

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

/**
 * Validates that a parameter can be converted to a positive {@code long} greater than zero.
 */
public class PositiveNonZeroLong implements IParameterValidator {

  @Override
  public void validate(String name, String value) throws ParameterException {
    long n = Long.parseLong(value);
    if (n <= 0) {
      throw new ParameterException("Parameter " + name + " should be greater than zero (found " + value + ")");
    }
  }

}
