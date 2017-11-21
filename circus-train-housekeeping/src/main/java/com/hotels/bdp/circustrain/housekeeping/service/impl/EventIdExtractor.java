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
package com.hotels.bdp.circustrain.housekeeping.service.impl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;

public final class EventIdExtractor {

  static final String EVENT_ID_REGEXP = ".+\\/(ct(?:t|p)-20[0-9]{2}[0-1][0-9][0-3][0-9]T[0-2][0-9][0-5][0-9][0-5][0-9](?:.|-)[a-zA-Z0-9]{4}(?:-)?[a-zA-Z0-9]{8})(\\/.*)?";

  private EventIdExtractor() {}

  public static String extractFrom(Path path) {
    Pattern pattern = Pattern.compile(EVENT_ID_REGEXP, Pattern.CASE_INSENSITIVE);
    Matcher matcher = pattern.matcher(path.toUri().toString());
    if (!matcher.matches()) {
      return null;
    }
    return matcher.group(1);
  }
}
