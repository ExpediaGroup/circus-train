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
package com.hotels.bdp.circustrain.gcp;

import fm.last.commons.test.file.TemporaryFolder;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doReturn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.gcp.context.GCPSecurity;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ FileSystem.class })
public class GCPCredentialCopierTest {

  private @Rule TemporaryFolder temporaryFolder = new TemporaryFolder();
  private @Mock FileSystem fs;
  private Configuration conf = new Configuration();
  private GCPSecurity gcpSecurity = new GCPSecurity();

  private void setGcpSecurity(
      String credentialProvider,
      String localFileSystemWorkingDirectory,
      String distributedFileSystemWorkingDirectory) {
    gcpSecurity.setCredentialProvider(credentialProvider);
    gcpSecurity.setLocalFileSystemWorkingDirectory(localFileSystemWorkingDirectory);
    gcpSecurity.setDistributedFileSystemWorkingDirectory(distributedFileSystemWorkingDirectory);
  }

  @Test(expected = IllegalArgumentException.class)
  public void credentialProviderNotSetThrowsException() throws Exception {
    doNothing().when(fs).copyFromLocalFile(any(Path.class), any(Path.class));
    doReturn(false).when(fs).exists(any(Path.class));
    GCPCredentialCopier copier = new GCPCredentialCopier(fs, conf, gcpSecurity);
  }

  @Test
  public void copyCredentialsWithOnlyCredentialProviderSupplied() throws Exception {
    doNothing().when(fs).copyFromLocalFile(any(Path.class), any(Path.class));
    String credentialProvider = "test.json";
    temporaryFolder.newFile(credentialProvider);
    credentialProvider = temporaryFolder.getRoot() + "/" + credentialProvider;
    setGcpSecurity(credentialProvider, null, null);
    GCPCredentialCopier copier = new GCPCredentialCopier(fs, conf, gcpSecurity);
    copier.copyCredentials();
    verify(fs, times(1)).copyFromLocalFile(any(Path.class), any(Path.class));
    assertNotNull(conf.get("mapreduce.job.cache.files"));
  }

  @Test(expected = CircusTrainException.class)
  public void copyCredentialsWhenCredentialProviderFileDoesntExistThrowsException() throws Exception {
    doNothing().when(fs).copyFromLocalFile(any(Path.class), any(Path.class));
    String credentialProvider = "test.json";
    setGcpSecurity(credentialProvider, null, null);
    GCPCredentialCopier copier = new GCPCredentialCopier(fs, conf, gcpSecurity);
    copier.copyCredentials();
  }

  @Test
  public void fullConfigurationProvided() throws Exception {
    doNothing().when(fs).copyFromLocalFile(any(Path.class), any(Path.class));
    String credentialProvider = "test.json";
    String localFileSystem = "workdir";
    String distributedFileSystem = "hdfs:/tmp/circus-train-gcp/workdir/";
    temporaryFolder.newFile(credentialProvider);
    temporaryFolder.newFolder(localFileSystem);
    credentialProvider = temporaryFolder.getRoot() + "/" + credentialProvider;
    setGcpSecurity(credentialProvider, localFileSystem, distributedFileSystem);
    GCPCredentialCopier copier = new GCPCredentialCopier(fs, conf, gcpSecurity);

    ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);
    copier.copyCredentials();
    verify(fs, times(1)).copyFromLocalFile(eq(new Path(credentialProvider)), pathCaptor.capture());
    Path destination = pathCaptor.getValue();
    assertTrue(destination.toString().startsWith(distributedFileSystem));
  }

}
