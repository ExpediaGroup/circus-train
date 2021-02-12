/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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
package com.hotels.bdp.circustrain;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.core.env.ConfigurableEnvironment;

@RunWith(MockitoJUnitRunner.class)
public class ConfigFileValidationApplicationListenerTest {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private ConfigFileValidationApplicationListener listener = new ConfigFileValidationApplicationListener();

  @Mock
  private ApplicationEnvironmentPreparedEvent event;
  @Mock
  private ConfigurableEnvironment env;

  @Before
  public void before() {
    when(event.getEnvironment()).thenReturn(env);
  }

  @Test
  public void emptyProperty() {
    when(env.getProperty(anyString())).thenReturn("");

    try {
      listener.onApplicationEvent(event);
      fail();
    } catch (ConfigFileValidationException e) {
      String error = e.getErrors().get(0).getDefaultMessage();
      assertThat(error, is("No config file was specified."));
    }
  }

  @Test
  public void configFileDoesNotExist() {
    File file = new File(temp.getRoot(), "application.yml");
    when(env.getProperty(anyString())).thenReturn(file.getAbsolutePath());

    try {
      listener.onApplicationEvent(event);
      fail();
    } catch (ConfigFileValidationException e) {
      String error = e.getErrors().get(0).getDefaultMessage();
      assertThat(error, containsString("Config file " + file.getAbsolutePath() + " does not exist."));
    }
  }

  @Test
  public void configFileIsDirectory() throws IOException {
    File folder = temp.newFolder("application.yml");
    when(env.getProperty(anyString())).thenReturn(folder.getAbsolutePath());

    try {
      listener.onApplicationEvent(event);
      fail();
    } catch (ConfigFileValidationException e) {
      String error = e.getErrors().get(0).getDefaultMessage();
      assertThat(error, containsString("Config file " + folder.getAbsolutePath() + " is a directory."));
    }
  }

  @Test
  public void configFileIsNotReadable() throws IOException {
    File file = temp.newFile("application.yml");
    file.setReadable(false);
    when(env.getProperty(anyString())).thenReturn(file.getAbsolutePath());

    try {
      listener.onApplicationEvent(event);
      fail();
    } catch (ConfigFileValidationException e) {
      String error = e.getErrors().get(0).getDefaultMessage();
      assertThat(error, containsString("Config file " + file.getAbsolutePath() + " cannot be read."));
    }
  }

  @Test
  public void configFileValid() throws IOException {
    File file = temp.newFile("application.yml");
    when(env.getProperty(anyString())).thenReturn(file.getAbsolutePath());

    listener.onApplicationEvent(event);
  }

}
