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
package com.hotels.bdp.circustrain;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.springframework.validation.ObjectError;

public class CircusTrainHelpTest {

  @Test
  public void typical() {
    String expectedHelpMessage = "Usage: circus-train.sh --config=<config_file>[,<config_file>,...]\n"
        + "Errors found in the provided configuration file:\n"
        + "\tError message 1\n"
        + "\tError message 2\n"
        + "Configuration file help:\n"
        + "\tFor more information and help please refer to https://stash/projects/HDW/repos/circus-train/browse/README.md";
    List<ObjectError> errors = Arrays.asList(new ObjectError("object.1", "Error message 1"),
        new ObjectError("object.2", "Error message 2"));
    String help = new CircusTrainHelp(errors).toString();
    assertThat(help, is(expectedHelpMessage));
  }

  @Test
  public void convertErrorToTabPrependedString() {
    ObjectError error = new ObjectError("object.name", "this an error message");
    String errorMessage = CircusTrainHelp.OBJECT_ERROR_TO_TABBED_MESSAGE.apply(error);
    assertThat(errorMessage, is("\tthis an error message"));
  }

}
