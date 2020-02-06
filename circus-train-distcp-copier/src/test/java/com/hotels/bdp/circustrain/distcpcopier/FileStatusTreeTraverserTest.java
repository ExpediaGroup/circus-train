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
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Iterables;

import com.hotels.bdp.circustrain.api.CircusTrainException;

@RunWith(MockitoJUnitRunner.class)
public class FileStatusTreeTraverserTest {

  @Mock
  private FileSystem fileSystem;
  @Mock
  private FileStatus status;

  private FileStatusTreeTraverser traverser;

  @Before
  public void before() {
    traverser = new FileStatusTreeTraverser(fileSystem);
  }

  @Test
  public void typical() throws IOException {
    when(status.isFile()).thenReturn(false);
    FileStatus fileStatus = new FileStatus();
    when(fileSystem.listStatus(any(Path.class))).thenReturn(new FileStatus[] { fileStatus });

    Iterable<FileStatus> children = traverser.children(status);

    verify(fileSystem).listStatus(any(Path.class));

    assertThat(Iterables.size(children), is(1));
    assertTrue(children.iterator().next() == fileStatus);
  }

  @Test
  public void noChildren() throws IOException {
    when(status.isFile()).thenReturn(false);
    when(fileSystem.listStatus(any(Path.class))).thenReturn(new FileStatus[] {});

    Iterable<FileStatus> children = traverser.children(status);

    verify(fileSystem).listStatus(any(Path.class));

    assertThat(Iterables.size(children), is(0));
  }

  @Test
  public void nullListStatus() throws IOException {
    when(status.isFile()).thenReturn(false);
    when(fileSystem.listStatus(any(Path.class))).thenReturn(null);

    Iterable<FileStatus> children = traverser.children(status);

    verify(fileSystem).listStatus(any(Path.class));

    assertThat(Iterables.size(children), is(0));
  }

  @Test
  public void rootIsFile() throws IOException {
    when(status.isFile()).thenReturn(true);

    Iterable<FileStatus> children = traverser.children(status);

    verify(fileSystem, never()).listStatus(any(Path.class));

    assertThat(Iterables.size(children), is(0));
  }

  @Test(expected = CircusTrainException.class)
  public void fileSystemException() throws IOException {
    when(status.isFile()).thenReturn(false);
    doThrow(IOException.class).when(fileSystem).listStatus(any(Path.class));

    traverser.children(status);
  }

}
