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
package com.hotels.bdp.circustrain.tool.vacuum;

import java.util.List;

import javax.annotation.Nonnull;

import org.springframework.validation.ObjectError;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;

class VacuumToolHelp {

  private static final String TAB = "\t";

  final static Function<ObjectError, String> OBJECT_ERROR_TO_TABBED_MESSAGE = new Function<ObjectError, String>() {
    @Override
    public String apply(@Nonnull ObjectError error) {
      return TAB + error.getDefaultMessage();
    }
  };

  private final List<ObjectError> errors;

  VacuumToolHelp(List<ObjectError> errors) {
    this.errors = errors;
  }

  @Override
  public String toString() {
    Iterable<String> errorMessages = FluentIterable.from(errors).transform(OBJECT_ERROR_TO_TABBED_MESSAGE);

    StringBuilder help = new StringBuilder(500)
        .append("Usage: vacuum.sh --config=<config_file>[,<config_file>,...] [--dry-run=true]")
        .append(System.lineSeparator())
        .append("Errors found in the provided configuration file:")
        .append(System.lineSeparator())
        .append(Joiner.on(System.lineSeparator()).join(errorMessages))
        .append(System.lineSeparator())
        .append("Configuration file help:")
        .append(System.lineSeparator())
        .append(TAB)
        .append("For more information and help please refer to ")
        .append("https://github.com/HotelsDotCom/circus-train/tree/master/circus-train-tool-parent");
    return help.toString();
  }
}
