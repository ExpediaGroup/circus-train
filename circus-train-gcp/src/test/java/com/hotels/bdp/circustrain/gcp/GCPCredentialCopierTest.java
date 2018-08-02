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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
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
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.gcp.context.GCPSecurity;

@RunWith(MockitoJUnitRunner.class)
public class GCPCredentialCopierTest {

  private static final String DISTRIBUTED_CACHE_PROPERTY = "mapreduce.job.cache.files";
  private static final String SYMLINK_FLAG = "#";

  private @Mock FileSystem fileSystem;
  private @Mock CredentialProviderRelativePathFactory credentialProviderRelativePathFactory;
  private @Mock HdfsGsCredentialDirectoryFactory hdfsGsCredentialDirectoryFactory;
  private @Mock RandomStringFactory randomStringFactory;
  private final Configuration conf = new Configuration();
  private final GCPSecurity gcpSecurity = new GCPSecurity();
  private GCPCredentialCopier copier;
  private final String credentialsFilePath = "/test.json";
  private final String credentialsFileRelativePath = ".." + credentialsFilePath;
  private final String randomString = "randomString";
  private final String hdfsDirectory = "/rootDirectory/test-" + randomString;
  private final String hdfsAbsolutePath = hdfsDirectory + "/" + GCPCredentialCopier.GCP_KEY_NAME;

  @Before
  public void init() {
    setGcpSecurity(credentialsFilePath, null);
    doReturn(randomString).when(randomStringFactory).newInstance();
    doReturn(credentialsFileRelativePath).when(credentialProviderRelativePathFactory).newInstance(gcpSecurity);
    doReturn(new Path(hdfsDirectory)).when(hdfsGsCredentialDirectoryFactory).newInstance(gcpSecurity);
    copier = new GCPCredentialCopier(fileSystem, conf, gcpSecurity, credentialProviderRelativePathFactory,
        hdfsGsCredentialDirectoryFactory);
  }

  private void setGcpSecurity(String credentialProvider, String distributedFileSystemWorkingDirectory) {
    gcpSecurity.setCredentialProvider(credentialProvider);
    gcpSecurity.setDistributedFileSystemWorkingDirectory(distributedFileSystemWorkingDirectory);
  }

  @Test(expected = IllegalArgumentException.class)
  public void credentialProviderNotSetThrowsException() throws Exception {
    doReturn(false).when(fileSystem).exists(any(Path.class));
    setGcpSecurity(null, null);
    new GCPCredentialCopier(fileSystem, conf, gcpSecurity);
  }

  @Test
  public void copyCredentialsWithCredentialProviderSupplied() throws Exception {
    copier.copyCredentials();
    verify(fileSystem).copyFromLocalFile(new Path(credentialsFileRelativePath), new Path(hdfsAbsolutePath));
    verify(fileSystem).deleteOnExit(new Path(hdfsDirectory));
    assertNotNull(conf.get(DISTRIBUTED_CACHE_PROPERTY));
    assertThat(conf.get(DISTRIBUTED_CACHE_PROPERTY), is(hdfsAbsolutePath + SYMLINK_FLAG + credentialsFileRelativePath));
    assertFalse(fileSystem.exists(new Path(hdfsAbsolutePath)));
  }

  @Test(expected = CircusTrainException.class)
  public void copyCredentialsWhenFileDoesntExistThrowsException() throws Exception {
    doThrow(new IOException("foo")).when(fileSystem).copyFromLocalFile(any(Path.class), any(Path.class));
    GCPCredentialCopier copier = new GCPCredentialCopier(fileSystem, conf, gcpSecurity);
    copier.copyCredentials();
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullGCPSecurityThrowsException() throws Exception {
    GCPCredentialCopier copier = new GCPCredentialCopier(fileSystem, conf, null);
    copier.copyCredentials();
  }

}
