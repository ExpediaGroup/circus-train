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
package com.hotels.bdp.circustrain.distcpcopier;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.io.Files;

import com.hotels.bdp.circustrain.api.CircusTrainException;

public class CircusTrainCopyListingTest {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private final Configuration conf = new Configuration();

  @Test
  public void copyListingClass() {
    CircusTrainCopyListing.setAsCopyListingClass(conf);

    assertThat(conf.get(DistCpConstants.CONF_LABEL_COPY_LISTING_CLASS), is(CircusTrainCopyListing.class.getName()));
  }

  @Test
  public void rootPath() {
    Path path = new Path("/foo");

    CircusTrainCopyListing.setRootPath(conf, path);

    assertThat(CircusTrainCopyListing.getRootPath(conf), is(path));
  }

  @Test(expected = CircusTrainException.class)
  public void rootPathNotSet() {
    CircusTrainCopyListing.getRootPath(conf);
  }

  @Test
  public void typical() throws IOException {
    File input = temp.newFolder("input");
    File inputSub2 = new File(input, "sub1/sub2");
    inputSub2.mkdirs();
    Files.asCharSink(new File(inputSub2, "data"), UTF_8).write("test1");

    File listFile = temp.newFile("listFile");
    Path pathToListFile = new Path(listFile.toURI());

    List<Path> sourceDataLocations = new ArrayList<>();
    sourceDataLocations.add(new Path(inputSub2.toURI()));
    DistCpOptions options = new DistCpOptions(sourceDataLocations, new Path("dummy"));

    CircusTrainCopyListing.setRootPath(conf, new Path(input.toURI()));
    CircusTrainCopyListing copyListing = new CircusTrainCopyListing(conf, null);
    copyListing.doBuildListing(pathToListFile, options);

    try (Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(pathToListFile))) {
      Text key = new Text();
      CopyListingFileStatus value = new CopyListingFileStatus();

      assertTrue(reader.next(key, value));
      assertThat(key.toString(), is("/sub1/sub2"));
      assertThat(value.getPath().toUri().toString(), endsWith("/input/sub1/sub2"));

      assertTrue(reader.next(key, value));
      assertThat(key.toString(), is("/sub1/sub2/data"));
      assertThat(value.getPath().toUri().toString(), endsWith("/input/sub1/sub2/data"));

      assertFalse(reader.next(key, value));

    }
  }

}
