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
package com.hotels.bdp.circustrain.distcpcopier;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.distcpcopier.RelativePathFunction;

@RunWith(MockitoJUnitRunner.class)
public class RelativePathFunctionTest {

  @Mock
  private FileStatus fileStatus;

  @Test
  public void typical() {
    Path sourceRootPath = new Path("/root/");
    Path path = new Path("/root/foo/bar/");
    when(fileStatus.getPath()).thenReturn(path);

    String relativePath = new RelativePathFunction(sourceRootPath).apply(fileStatus);

    assertThat(relativePath, is("/foo/bar"));
  }
}
