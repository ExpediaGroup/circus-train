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

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
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
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import fm.last.commons.test.file.TemporaryFolder;

import com.hotels.bdp.circustrain.api.CircusTrainException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ FileSystem.class })
public class GCPCredentialCopierTest {

  private @Rule TemporaryFolder temporaryFolder = new TemporaryFolder();
  private @Mock FileSystem fs;
  private Configuration conf = new Configuration();

  @Test(expected = IllegalArgumentException.class)
  public void credentialProviderNotSetThrowsException() throws Exception {
    doNothing().when(fs).copyFromLocalFile(any(Path.class), any(Path.class));
    doReturn(false).when(fs).exists(any(Path.class));
    String credentialProvider = null;
    GCPCredentialCopier copier = new GCPCredentialCopier(fs, conf, credentialProvider);
  }

  @Test
  public void copyCredentials() throws Exception {
    doNothing().when(fs).copyFromLocalFile(any(Path.class), any(Path.class));
    String credentialProvider = "test.json";
    temporaryFolder.newFile(credentialProvider);
    credentialProvider = temporaryFolder.getRoot() + "/" + credentialProvider;
    GCPCredentialCopier copier = new GCPCredentialCopier(fs, conf, credentialProvider);
    copier.copyCredentials();
    verify(fs, times(1)).copyFromLocalFile(any(Path.class), any(Path.class));
    assertNotNull(conf.get("mapreduce.job.cache.files"));
  }

  @Test(expected = CircusTrainException.class)
  public void copyCredentialsWhenFileDoesntExistThrowsException() throws Exception {
    doNothing().when(fs).copyFromLocalFile(any(Path.class), any(Path.class));
    String credentialProvider = "test.json";
    GCPCredentialCopier copier = new GCPCredentialCopier(fs, conf, credentialProvider);
    copier.copyCredentials();
  }
}