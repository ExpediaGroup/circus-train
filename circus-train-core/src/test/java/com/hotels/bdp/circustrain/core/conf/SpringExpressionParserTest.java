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
package com.hotels.bdp.circustrain.core.conf;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

public class SpringExpressionParserTest {

  private static class StubExpressionParserFunctions {
    @SuppressWarnings("unused")
    public static DateTime now() {
      return new DateTime(2016, 3, 24, 0, 0, 0, 0);
    }

    @SuppressWarnings("unused")
    public static DateTime nowUtc() {
      return new DateTime(2016, 3, 24, 0, 0, 0, 0, DateTimeZone.UTC);
    }
  }

  private final SpringExpressionParser parser = new SpringExpressionParser(StubExpressionParserFunctions.class);

  @Test
  public void literal() {
    assertThat(parser.parse("local_hour > 0"), is("local_hour > 0"));
  }

  @Test
  public void empty() {
    assertThat(parser.parse(""), is(""));
  }

  @Test
  public void blank() {
    assertThat(parser.parse("  "), is("  "));
  }

  @Test
  public void nullValue() {
    assertThat(parser.parse(null), is((Object) null));
  }

  @Test
  public void literalWithNowFunction() {
    assertThat(parser.parse("local_date > #{ #now().minusDays(30).toString('yyyy-MM-dd')}"),
        is("local_date > 2016-02-23"));
  }

  @Test
  public void spaceBeforeRootContextReference() {
    assertThat(parser.parse("local_date >= '#{ #nowUtc().minusDays(2).toString(\"yyyy-MM-dd\") }'"),
        is("local_date >= '2016-03-22'"));
  }

}
