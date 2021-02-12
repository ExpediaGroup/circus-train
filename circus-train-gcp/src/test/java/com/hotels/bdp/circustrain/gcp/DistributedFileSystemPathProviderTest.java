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
package com.hotels.bdp.circustrain.gcp;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doReturn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.gcp.context.GCPSecurity;

@RunWith(MockitoJUnitRunner.class)
public class DistributedFileSystemPathProviderTest {

  private final GCPSecurity security = new GCPSecurity();
  private @Mock RandomStringFactory randomStringFactory;
  private @Mock Configuration configuration;

  private final String randomString = "test";

  @Test
  public void newInstanceWithoutConfigurationSet() {
    String dfsDirectory = "hdfs:/test-dir/";
    doReturn(randomString).when(randomStringFactory).newInstance();
    doReturn(dfsDirectory).when(configuration).get("hive.exec.scratchdir");
    Path directory = new DistributedFileSystemPathProvider(security, randomStringFactory).newPath(configuration);
    String baseDirectoryExpected = dfsDirectory + randomString;
    assertThat(directory, is(new Path(baseDirectoryExpected, DistributedFileSystemPathProvider.GCP_KEY_NAME)));
  }

  @Test
  public void newInstanceWithConfigurationSet() {
    String providedDirectory = "hdfs:/test/directory";
    doReturn(randomString).when(randomStringFactory).newInstance();
    security.setDistributedFileSystemWorkingDirectory(providedDirectory);
    Path directory = new DistributedFileSystemPathProvider(security, randomStringFactory).newPath(configuration);
    assertThat(directory.toString(),
        is(providedDirectory + "/" + randomString + "/" + DistributedFileSystemPathProvider.GCP_KEY_NAME));
  }

  @Test
  public void newInstanceWithoutHiveConfScratchDir() {
    doReturn("").when(configuration).get("hive.exec.scratchdir");
    doReturn(randomString).when(randomStringFactory).newInstance();
    Path directory = new DistributedFileSystemPathProvider(security, randomStringFactory).newPath(configuration);
    String baseDirectoryExpected = DistributedFileSystemPathProvider.DEFAULT_HDFS_PREFIX + randomString;
    assertThat(directory, is(new Path(baseDirectoryExpected, DistributedFileSystemPathProvider.GCP_KEY_NAME)));
  }

}
