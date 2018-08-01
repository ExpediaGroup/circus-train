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
package com.hotels.bdp.circustrain.gcp;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.gcp.context.GCPSecurity;

@RunWith(MockitoJUnitRunner.class)
public class HdfsGsCredentialDirectoryFactoryTest {

  private @Mock GCPSecurity security;

  @Test
  public void newInstanceWithoutConfigurationSet() {
    doReturn("").when(security).getDistributedFileSystemWorkingDirectory();
    String randomString = "test";
    Path directory = new HdfsGsCredentialDirectoryFactory().newInstance(security, randomString);
    assertThat(directory.toString(), is(HdfsGsCredentialDirectoryFactory.DEFAULT_HDFS_PREFIX + randomString));
  }

  @Test
  public void newInstanceWithConfigurationSet() {
    String providedDirectory = "hdfs:/test/directory";
    doReturn(providedDirectory).when(security).getDistributedFileSystemWorkingDirectory();
    String randomString = "test";
    Path directory = new HdfsGsCredentialDirectoryFactory().newInstance(security, randomString);
    assertThat(directory.toString(), is(providedDirectory + randomString));
  }

}
