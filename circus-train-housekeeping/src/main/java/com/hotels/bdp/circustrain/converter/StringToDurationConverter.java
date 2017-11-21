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
package com.hotels.bdp.circustrain.converter;

import org.joda.time.Duration;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.springframework.core.convert.converter.Converter;

/**
 * Converts a {@link String} property into a {@link Duration} object.
 * <p>
 * The property must be formatted in <a href="https://en.wikipedia.org/wiki/ISO_8601#Durations">/ISO 8601</a>.
 * </p>
 * <p>
 * Unfortunately Joda {@link Duration#parse(String)} doesn't fully support ISO 8601 so this {@link Converter} uses its
 * own version which only supports up to days and no P or T delimiters.
 * </p>
 */
// Note this class is generic and could be moved to a more generic project
public class StringToDurationConverter implements Converter<String, Duration> {

  private static final PeriodFormatter FORMATTER = new PeriodFormatterBuilder()
      .printZeroNever()
      .appendLiteral("P")
      .appendDays()
      .appendSuffix("D")
      .appendSeparatorIfFieldsBefore("T")
      .appendHours()
      .appendSuffix("H")
      .appendMinutes()
      .appendSuffix("M")
      .appendSeconds()
      .appendSuffix("S")
      .toFormatter();

  @Override
  public Duration convert(String source) {
    return FORMATTER.parsePeriod(source).toStandardDuration();
  }

}
