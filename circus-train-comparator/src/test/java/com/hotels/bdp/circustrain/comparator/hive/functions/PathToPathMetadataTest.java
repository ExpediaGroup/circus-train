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
package com.hotels.bdp.circustrain.comparator.hive.functions;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.comparator.hive.wrappers.PathMetadata;

@RunWith(MockitoJUnitRunner.class)
public class PathToPathMetadataTest {

  private static final String DIR_PATH = "file:/abc";
  private static final String FILE_PATH = DIR_PATH + "/child.dat";
  private static final long LAST_MODIFIED = 123456789L;
  private static final long LAST_MODIFIED_CHILD = 999999999999L;
  private static final String CHECKSUM_ALGORITHM = "CHECKSUM_ALGORITHM";
  private static final int CHECKSUM_LENGTH = 64;
  private static final byte[] CHECKSUM_BYTES = new byte[] { 1, 2, 3 };

  private @Mock Path path;
  private @Mock FileSystem fs;
  private @Mock FileStatus fileStatus;
  private @Mock FileChecksum fileChecksum;

  private PathToPathMetadata function;

  @Before
  public void init() throws Exception {
    when(path.toUri()).thenReturn(new URI(DIR_PATH));
    when(path.getFileSystem(any(Configuration.class))).thenReturn(fs);

    when(fileStatus.getModificationTime()).thenReturn(LAST_MODIFIED);
    when(fs.getFileStatus(path)).thenReturn(fileStatus);

    when(fileChecksum.getAlgorithmName()).thenReturn(CHECKSUM_ALGORITHM);
    when(fileChecksum.getLength()).thenReturn(CHECKSUM_LENGTH);
    when(fileChecksum.getBytes()).thenReturn(CHECKSUM_BYTES);

    function = new PathToPathMetadata(new Configuration());
  }

  @Test
  public void file() throws Exception {
    when(fileStatus.isFile()).thenReturn(true);
    when(fileStatus.isDirectory()).thenReturn(false);
    when(fs.getFileChecksum(path)).thenReturn(fileChecksum);

    PathMetadata metadata = function.apply(path);

    assertThat(metadata.getLocation(), is(DIR_PATH));
    assertThat(metadata.getLastModifiedTimestamp(), is(LAST_MODIFIED));
    assertThat(metadata.getChecksumAlgorithmName(), is(CHECKSUM_ALGORITHM));
    assertThat(metadata.getChecksumLength(), is(CHECKSUM_LENGTH));
    assertThat(metadata.getChecksum(), is(CHECKSUM_BYTES));
    assertThat(metadata.getChildrenMetadata().isEmpty(), is(true));
    verify(fs, never()).listStatus(any(Path.class));
  }

  @Test
  public void directory() throws Exception {
    when(fileStatus.isFile()).thenReturn(false);
    when(fileStatus.isDirectory()).thenReturn(true);

    Path childPath = mock(Path.class);
    when(childPath.toUri()).thenReturn(new URI(FILE_PATH));
    when(childPath.getFileSystem(any(Configuration.class))).thenReturn(fs);

    FileStatus childStatus = mock(FileStatus.class);
    when(childStatus.getPath()).thenReturn(childPath);
    when(childStatus.getModificationTime()).thenReturn(LAST_MODIFIED_CHILD);
    when(childStatus.isFile()).thenReturn(true);
    when(childStatus.isDirectory()).thenReturn(false);
    when(fs.getFileStatus(childPath)).thenReturn(childStatus);
    when(fs.getFileChecksum(childPath)).thenReturn(fileChecksum);

    FileStatus[] childStatuses = new FileStatus[] { childStatus };
    when(fs.listStatus(path)).thenReturn(childStatuses);

    PathMetadata metadata = function.apply(path);

    assertThat(metadata.getLocation(), is(DIR_PATH));
    assertThat(metadata.getLastModifiedTimestamp(), is(0L));
    assertThat(metadata.getChecksumAlgorithmName(), is(nullValue()));
    assertThat(metadata.getChecksumLength(), is(0));
    assertThat(metadata.getChecksum(), is(nullValue()));
    assertThat(metadata.getChildrenMetadata().size(), is(1));
    verify(fs, times(1)).listStatus(path);
    verify(fs, never()).getFileChecksum(path);

    PathMetadata childMetadata = metadata.getChildrenMetadata().get(0);
    assertThat(childMetadata.getLocation(), is(FILE_PATH));
    assertThat(childMetadata.getLastModifiedTimestamp(), is(LAST_MODIFIED_CHILD));
    assertThat(childMetadata.getChecksumAlgorithmName(), is(CHECKSUM_ALGORITHM));
    assertThat(childMetadata.getChecksumLength(), is(CHECKSUM_LENGTH));
    assertThat(childMetadata.getChecksum(), is(CHECKSUM_BYTES));
    assertThat(childMetadata.getChildrenMetadata().size(), is(0));
    verify(fs, never()).listStatus(childPath);
    verify(fs, times(1)).getFileChecksum(childPath);
  }

}
