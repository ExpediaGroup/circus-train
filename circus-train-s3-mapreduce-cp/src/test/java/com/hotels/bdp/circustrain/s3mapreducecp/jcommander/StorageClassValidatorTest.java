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

public class StorageClassValidatorTest {

  private final StorageClassValidator validator = new StorageClassValidator();

  @Test
  public void typical() {
    validator.validate("storageClass", "standard");
    validator.validate("storageClass", "STANDARD");
    validator.validate("storageClass", "STaNDaRD");
  }

  @Test
  public void nullValue() {
    validator.validate("storageClass", null);
  }

  @Test(expected = ParameterException.class)
  public void invalid() {
    validator.validate("storageClass", "REDUCEDREDUNDANCY");
  }

}
