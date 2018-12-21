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
package com.hotels.bdp.circustrain.core;

import java.util.Locale;

import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Generate an id that will to all intents and purposes be unique for a given replication task. This is an interface so
 * that we can mock and return predictable values for tests. This id is generally used for the creation of unique,
 * non-conflicting file system objects, but also to allow easy tracing of events in log files. A time element is
 * included to assist in debugging.
 * <p/>
 * Care must be taken to ensure that generated IDs do not exceed the maximum allowed length for a snapshot name and a
 * {@link org.apache.hadoop.fs.Path Path} element. In both cases I've been unable to find the imposed limits. Therefore
 * the length of the ID generated is conservative, which should also make it easier to read in any logs.
 */
public interface EventIdFactory {

  public static final DateTimeFormatter FORMATTER = ISODateTimeFormat.basicDateTime().withZoneUTC();
  public static final EventIdFactory DEFAULT = new EventIdFactory() {

    /**
     * Example:
     *
     * <pre>
     * ctt-20160322T061345.487Z-YtMhVotF
     * </pre>
     */
    @Override
    public String newEventId(String prefix) {
      // These will appear in folder paths, snapshot names, etc
      // Must be lower case for Google Cloud Storage
      String id = prefix
          + "-"
          + FORMATTER.print(System.currentTimeMillis())
          + "-"
          + RandomStringUtils.randomAlphanumeric(8);
      id = id.toLowerCase(Locale.ROOT);
      return id;
    }
  };

  String newEventId(String prefix);
}
