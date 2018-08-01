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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.gcp.context.GCPSecurity;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ FileSystem.class })
public class GCPCredentialConfigurerTest {

  private @Rule final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private @Mock FileSystem fileSystem;

  @Test
  public void configureCredentials() throws Exception {
    PowerMockito.mockStatic(FileSystem.class);
    Configuration conf = new Configuration();
    when(FileSystem.get(conf)).thenReturn(fileSystem);
    doNothing().when(fileSystem).copyFromLocalFile(any(Path.class), any(Path.class));
    String credentialProvider = "test.json";
    temporaryFolder.newFile(credentialProvider);
    credentialProvider = temporaryFolder.getRoot() + "/" + credentialProvider;
    GCPSecurity security = new GCPSecurity();
    security.setCredentialProvider(credentialProvider);
    GCPCredentialConfigurer configurer = new GCPCredentialConfigurer(conf, security);
    configurer.configureCredentials();
    verify(fileSystem, times(1)).copyFromLocalFile(any(Path.class), any(Path.class));
    assertNotNull(conf.get("mapreduce.job.cache.files"));
  }

  @Test(expected = CircusTrainException.class)
  public void configureCredentialsWithIncorrectPathThrowsException() throws Exception {
    PowerMockito.mockStatic(FileSystem.class);
    Configuration conf = new Configuration();
    when(FileSystem.get(conf)).thenReturn(fileSystem);
    doThrow(CircusTrainException.class).when(fileSystem).copyFromLocalFile(any(Path.class), any(Path.class));
    String credentialProvider = "test.json";
    GCPSecurity security = new GCPSecurity();
    security.setCredentialProvider(credentialProvider);
    GCPCredentialConfigurer configurer = new GCPCredentialConfigurer(conf, security);
    configurer.configureCredentials();
    verify(fileSystem, times(1)).copyFromLocalFile(any(Path.class), any(Path.class));
    assertNotNull(conf.get("mapreduce.job.cache.files"));
  }

  @Test(expected = CircusTrainException.class)
  public void fileSystemGetFailureThrowsException() throws Exception {
    PowerMockito.mockStatic(FileSystem.class);
    Configuration conf = null;
    when(FileSystem.get(conf)).thenThrow(IOException.class);
    String credentialProvider = "test.json";
    temporaryFolder.newFile(credentialProvider);
    credentialProvider = temporaryFolder.getRoot() + "/" + credentialProvider;
    GCPSecurity security = new GCPSecurity();
    security.setCredentialProvider(credentialProvider);
    GCPCredentialConfigurer configurer = new GCPCredentialConfigurer(conf, security);
    configurer.configureCredentials();
  }
}
