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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doReturn;

import org.apache.commons.io.FileUtils;
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

import fm.last.commons.test.file.TemporaryFolder;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.gcp.context.GCPSecurity;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ FileSystem.class })
public class GCPCredentialCopierTest {

  private @Rule final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private @Mock FileSystem fs;
  private @Mock FileUtils fileUtils;
  private final Configuration conf = new Configuration();
  private final GCPSecurity gcpSecurity = new GCPSecurity();

  private String credentialProvider = "test.json";
  private final String distributedFileSystem = "hdfs:/tmp/circus-train-gcp/workdir/";
  private final String defaultDistributedFileSystemRoot = "hdfs:/tmp/ct-gcp-";

  private void setGcpSecurity(String credentialProvider, String distributedFileSystemWorkingDirectory) {
    gcpSecurity.setCredentialProvider(credentialProvider);
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
    temporaryFolder.newFile(credentialProvider);
    credentialProvider = temporaryFolder.getRoot() + "/" + credentialProvider;
    setGcpSecurity(credentialProvider, null);
    GCPCredentialCopier copier = new GCPCredentialCopier(fs, conf, gcpSecurity);
    copier.copyCredentials();
    verify(fs).copyFromLocalFile(any(Path.class), any(Path.class));
    assertNotNull(conf.get("mapreduce.job.cache.files"));

  }

  @Test(expected = CircusTrainException.class)
  public void copyCredentialsWhenCredentialProviderFileDoesntExistThrowsException() throws Exception {

    setGcpSecurity(credentialProvider, null);
    GCPCredentialCopier copier = new GCPCredentialCopier(fs, conf, gcpSecurity);
    copier.copyCredentials();
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullGCPSecurity() throws Exception {
    doNothing().when(fs).copyFromLocalFile(any(Path.class), any(Path.class));
    GCPCredentialCopier copier = new GCPCredentialCopier(fs, conf, null);
    copier.copyCredentials();
  }

  @Test
  public void fullConfigurationProvided() throws Exception {
    doNothing().when(fs).copyFromLocalFile(any(Path.class), any(Path.class));
    temporaryFolder.newFile(credentialProvider);
    credentialProvider = temporaryFolder.getRoot() + "/" + credentialProvider;
    setGcpSecurity(credentialProvider, distributedFileSystem);
    GCPCredentialCopier copier = new GCPCredentialCopier(fs, conf, gcpSecurity);
    ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);
    copier.copyCredentials();

    verify(fs).copyFromLocalFile(eq(new Path(credentialProvider)), pathCaptor.capture());
    Path destination = pathCaptor.getValue();
    assertTrue(destination.toString().startsWith(distributedFileSystem));
  }

  @Test
  public void noDistributedFileSystemProvided() throws Exception {
    doNothing().when(fs).copyFromLocalFile(any(Path.class), any(Path.class));
    temporaryFolder.newFile(credentialProvider);
    credentialProvider = temporaryFolder.getRoot() + "/" + credentialProvider;
    setGcpSecurity(credentialProvider, null);
    GCPCredentialCopier copier = new GCPCredentialCopier(fs, conf, gcpSecurity);
    ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);
    copier.copyCredentials();
    verify(fs).copyFromLocalFile(eq(new Path(credentialProvider)), pathCaptor.capture());
    Path destination = pathCaptor.getValue();
    assertTrue(destination.toString().startsWith(defaultDistributedFileSystemRoot));
  }

}
