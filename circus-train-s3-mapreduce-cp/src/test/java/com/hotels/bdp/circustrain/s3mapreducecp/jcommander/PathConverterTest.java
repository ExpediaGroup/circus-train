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
package com.hotels.bdp.circustrain.s3mapreducecp.jcommander;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.hadoop.fs.Path;
import org.junit.Test;


public class PathConverterTest {

  private final PathConverter converter = new PathConverter();

  @Test
  public void typical() {
    assertThat(converter.convert("s3://bucket/foo/bar"), is(new Path("s3://bucket/foo/bar")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalid() {
    converter.convert("s3:");
  }

}
