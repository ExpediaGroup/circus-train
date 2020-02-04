/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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

import java.util.ArrayList;
import java.util.List;

import org.springframework.validation.ObjectError;

import com.hotels.bdp.circustrain.api.CircusTrainException;

class ConfigFileValidationException extends CircusTrainException {

  private static final long serialVersionUID = 1L;

  private List<ObjectError> errors = new ArrayList<>();

  ConfigFileValidationException(List<String> errors) {
    super("Error reading config file(s).");
    for (String error : errors) {
      this.errors.add(new ObjectError("config-file-error", error));
    }
  }

  List<ObjectError> getErrors() {
    return errors;
  }

}
