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
package com.hotels.bdp.circustrain.api.copier;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CopierPathGeneratorTest {

  private @Mock CopierPathGeneratorParams copierParams;

  @Before
  public void init() {
    when(copierParams.getOriginalSourceBaseLocation()).thenReturn(new Path("originalSourceBaseLocation"));
    when(copierParams.getOriginalReplicaLocation()).thenReturn(new Path("originalReplicaLocation"));
  }

  @Test
  public void identitySourceBaseLocation() {
    assertThat(CopierPathGenerator.IDENTITY.generateSourceBaseLocation(copierParams),
        is(new Path("originalSourceBaseLocation")));
  }

  @Test
  public void identityReplicaLocation() {
    assertThat(CopierPathGenerator.IDENTITY.generateReplicaLocation(copierParams),
        is(new Path("originalReplicaLocation")));
  }

}
