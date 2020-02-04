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
package com.hotels.bdp.circustrain.core.source;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class ViewLocationManagerTest {

  private ViewLocationManager locationManager = new ViewLocationManager();

  @Test
  public void nullTableLocation() {
    assertThat(locationManager.getTableLocation(), is(nullValue()));
  }

  @Test
  public void emptyPartitionLocations() {
    assertThat(locationManager.getPartitionLocations(), is(not(nullValue())));
  }

  @Test
  public void nullPartitionSubPath() {
    assertThat(locationManager.getPartitionSubPath(new Path("whatever")), is(nullValue()));
  }

}
