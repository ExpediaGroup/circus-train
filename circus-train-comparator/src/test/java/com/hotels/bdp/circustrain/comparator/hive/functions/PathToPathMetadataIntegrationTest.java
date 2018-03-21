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
package com.hotels.bdp.circustrain.comparator.hive.functions;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.io.Files;

import com.hotels.bdp.circustrain.comparator.hive.wrappers.PathMetadata;

@RunWith(MockitoJUnitRunner.class)
public class PathToPathMetadataIntegrationTest {

  public @Rule TemporaryFolder tmp = new TemporaryFolder();

  private static final int NUM_OF_DATA_FILES = 3;
  private static final String DIR = "my-data";
  private static final String FILE_NAME_TEMPLATE = "part-m%05d";

  private final PathToPathMetadata function = new PathToPathMetadata(new Configuration());

  private File baseDir;

  @Before
  public void init() throws Exception {
    baseDir = tmp.newFolder(DIR);
    for (int i = 0; i < NUM_OF_DATA_FILES; ++i) {
      File dataFile = new File(baseDir, String.format(FILE_NAME_TEMPLATE, i));
      Files.asCharSink(dataFile, StandardCharsets.UTF_8).writeLines(Arrays.asList("data " + i));
    }
  }

  @Test
  public void file() throws Exception {
    Path path = new Path(baseDir.toURI());
    PathMetadata metadata = function.apply(path);

    assertThat(metadata.getLastModifiedTimestamp(), is(0L));
    assertThat(metadata.getLocation(), is(baseDir.toURI().toString()));
    assertThat(metadata.getChecksumAlgorithmName(), is(nullValue()));
    assertThat(metadata.getChecksumLength(), is(0));
    assertThat(metadata.getChecksum(), is(nullValue()));
    assertThat(metadata.getChildrenMetadata().size(), is(NUM_OF_DATA_FILES));

    for (int i = 0; i < NUM_OF_DATA_FILES; ++i) {
      File dataFile = new File(baseDir, String.format(FILE_NAME_TEMPLATE, i));

      PathMetadata childMetadata = getChildMetadata(metadata.getChildrenMetadata(), dataFile);
      assertThat(childMetadata.getLastModifiedTimestamp(), is(dataFile.lastModified()));
      assertThat(childMetadata.getChecksumAlgorithmName(), is(nullValue()));
      assertThat(childMetadata.getChecksumLength(), is(0));
      assertThat(childMetadata.getChecksum(), is(nullValue()));
      assertThat(childMetadata.getChildrenMetadata().size(), is(0));
    }
  }

  private PathMetadata getChildMetadata(List<PathMetadata> childrenMetadata, File dataFile) {
    String dataFileLocation = dataFile.getAbsolutePath().toString();
    for (PathMetadata childMetadata : childrenMetadata) {
      if (childMetadata.getLocation().replaceFirst("file://", "").equals(dataFileLocation)) {
        return childMetadata;
      }

    }
    fail(String.format("Data file %s not found", dataFile));
    return null;
  }

}
