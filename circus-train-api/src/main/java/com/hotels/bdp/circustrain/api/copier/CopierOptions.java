/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
package com.hotels.bdp.circustrain.api.copier;

import java.util.Map;

public interface CopierOptions {

  String IGNORE_MISSING_PARTITION_FOLDER_ERRORS = "ignore-missing-partition-folder-errors";

  // internal option used to track if the destination of the replication should be treated as a folder or file. Value
  // can be parsed with Boolean.parseValue. If not set a folder is assumed.
  String COPY_DESTINATION_IS_FILE = "copy-destination-is-file";

  Map<String, Object> getCopierOptions();

}
