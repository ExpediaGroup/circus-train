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
package com.hotels.bdp.circustrain.avro.util;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;

public final class AvroStringUtils {

  private AvroStringUtils() {}

  public static String avroDestination(String pathToDestinationFolder, String eventId, String tableLocation) {
    checkArgument(isNotBlank(pathToDestinationFolder), "There must be a pathToDestinationFolder provided");
    checkArgument(isNotBlank(eventId), "There must be a eventId provided");

    pathToDestinationFolder = appendForwardSlashIfNotPresent(pathToDestinationFolder);
    eventId = appendForwardSlashIfNotPresent(eventId);

    if (tableLocation != null && pathToDestinationFolder.equals(appendForwardSlashIfNotPresent(tableLocation))) {
      return pathToDestinationFolder + eventId + ".schema";
    }
    return pathToDestinationFolder + eventId;
  }

  public static boolean argsPresent(String... args) {
    for (String arg : args) {
      if (isBlank(arg)) {
        return false;
      }
    }
    return true;
  }

  @VisibleForTesting
  static String appendForwardSlashIfNotPresent(String path) {
    checkArgument(isNotBlank(path), "There must be a path provided");
    if (path.charAt(path.length() - 1) == '/') {
      return path;
    }
    return path + "/";
  }
}
