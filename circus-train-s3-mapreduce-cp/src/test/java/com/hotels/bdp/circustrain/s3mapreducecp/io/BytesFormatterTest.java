/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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
package com.hotels.bdp.circustrain.s3mapreducecp.io;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

public class BytesFormatterTest {

  @Test
  public void bytes() {
    assertThat(BytesFormatter.getStringDescriptionFor(100L), is("100.0B"));
  }

  @Test
  public void kilobytes() {
    assertThat(BytesFormatter.getStringDescriptionFor(1024L), is("1.0K"));
    assertThat(BytesFormatter.getStringDescriptionFor(2256L), is("2.2K"));
  }

  @Test
  public void megabytes() {
    assertThat(BytesFormatter.getStringDescriptionFor(1024L * 1024L), is("1.0M"));
    assertThat(BytesFormatter.getStringDescriptionFor((2024L * 1024L) + (256L * 1024L)), is("2.2M"));
  }

  @Test
  public void gigabytes() {
    assertThat(BytesFormatter.getStringDescriptionFor(1024L * 1024L * 1024L), is("1.0G"));
  }

  @Test
  public void terabytes() {
    assertThat(BytesFormatter.getStringDescriptionFor(1024L * 1024L * 1024L * 1024L), is("1.0T"));
  }

  @Test
  public void petabytes() {
    assertThat(BytesFormatter.getStringDescriptionFor(1024L * 1024L * 1024L * 1024L * 1024L), is("1.0P"));
  }

}
