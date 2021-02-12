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
import static org.junit.Assert.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.CircusTrainException;

@RunWith(MockitoJUnitRunner.class)
public class GCPCredentialCopierTest {

  private static final String DISTRIBUTED_CACHE_PROPERTY = "mapreduce.job.cache.files";
  private static final String SYMLINK_FLAG = "#";

  private final Configuration conf = new Configuration();
  private final Path credentialsFileRelativePath = new Path("../test.json");
  private final Path dfsDirectory = new Path("/rootDirectory/test-random-string");
  private final Path dfsAbsolutePath = new Path(dfsDirectory, DistributedFileSystemPathProvider.GCP_KEY_NAME);
  private final GCPCredentialCopier copier = new GCPCredentialCopier();

  private @Mock FileSystem fileSystem;
  private @Mock GCPCredentialPathProvider credentialPathProvider;
  private @Mock DistributedFileSystemPathProvider distributedFileSystemPathProvider;

  @Before
  public void init() {
    doReturn(credentialsFileRelativePath).when(credentialPathProvider).newPath();
    doReturn(dfsAbsolutePath).when(distributedFileSystemPathProvider).newPath(conf);
  }

  @Test
  public void copyCredentialsWithCredentialProviderSupplied() throws Exception {
    copier.copyCredentials(fileSystem, conf, credentialPathProvider, distributedFileSystemPathProvider);
    verify(fileSystem).copyFromLocalFile(credentialsFileRelativePath, dfsAbsolutePath);
    verify(fileSystem).deleteOnExit(dfsDirectory);
    assertNotNull(conf.get(DISTRIBUTED_CACHE_PROPERTY));
    assertThat(conf.get(DISTRIBUTED_CACHE_PROPERTY), is(dfsAbsolutePath + SYMLINK_FLAG + credentialsFileRelativePath));
  }

  @Test(expected = CircusTrainException.class)
  public void copyCredentialsWhenFileDoesntExistThrowsException() throws Exception {
    doThrow(new IOException("File does not exist"))
        .when(fileSystem)
        .copyFromLocalFile(any(Path.class), any(Path.class));
    copier.copyCredentials(fileSystem, conf, credentialPathProvider, distributedFileSystemPathProvider);
  }
}
