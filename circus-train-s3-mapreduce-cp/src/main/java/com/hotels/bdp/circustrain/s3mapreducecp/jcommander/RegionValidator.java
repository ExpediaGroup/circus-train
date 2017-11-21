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

import com.amazonaws.services.s3.model.Region;
import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

/**
 * Validates that a parameter can be converted to an AWS {@code Regions}.
 * <p>
 * Note this class assumes that values are region names instead of the enumeration name.
 * </p>
 */
public class RegionValidator implements IParameterValidator {

  @Override
  public void validate(String name, String value) throws ParameterException {
    try {
      Region.fromValue(value);
    } catch (IllegalArgumentException e) {
      throw new ParameterException("Parameter " + name + " is not a valid AWS region (found " + value + ")", e);
    }
  }

}
