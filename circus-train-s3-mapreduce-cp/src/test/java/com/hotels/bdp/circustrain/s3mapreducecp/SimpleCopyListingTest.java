/**
 * Copyright (C) 2016-2018 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.TestCopyListing}, {@code org.apache.hadoop.tools.TestFileBasedCopyListing} and {@code org.apache.hadoop.tools.TestGlobbedCopyListing} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/TestCopyListing.java
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/TestFileBasedCopyListing.java
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/TestGlobbedCopyListing.java
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
package com.hotels.bdp.circustrain.s3mapreducecp;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import static com.hotels.bdp.circustrain.s3mapreducecp.util.S3MapReduceCpTestUtils.createFile;
import static com.hotels.bdp.circustrain.s3mapreducecp.util.S3MapReduceCpTestUtils.delete;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.hotels.bdp.circustrain.s3mapreducecp.CopyListing.DuplicateFileException;
import com.hotels.bdp.circustrain.s3mapreducecp.CopyListing.InvalidInputException;
import com.hotels.bdp.circustrain.s3mapreducecp.util.PathUtil;

public class SimpleCopyListingTest {

  private static final Credentials CREDENTIALS = new Credentials();
  private Configuration config = new Configuration();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private SimpleCopyListing listing = new SimpleCopyListing(config, CREDENTIALS);
  private String temporaryRoot;

  @Before
  public void init() throws Exception {
    temporaryRoot = temporaryFolder.getRoot().getAbsolutePath();
  }

  @Test
  public void typical() throws Exception {
    Map<String, String> expectedValues = new HashMap<>();
    FileSystem fs = FileSystem.get(config);
    Path source = new Path(temporaryRoot + "/source");
    Path p1 = new Path(source, "1");
    Path p2 = new Path(source, "2");
    Path p3 = new Path(source, "2/3");
    Path p4 = new Path(source, "2/3/4");
    Path p5 = new Path(source, "5");
    Path p6 = new Path(source, "5/6");
    Path p7 = new Path(source, "7");
    Path p8 = new Path(source, "7/8");
    Path p9 = new Path(source, "7/8/9");
    fs.mkdirs(p1);
    fs.mkdirs(p2);
    fs.mkdirs(p3);
    fs.mkdirs(p4);
    fs.mkdirs(p5);
    createFile(fs, p6);
    expectedValues.put(fs.makeQualified(p6).toString(), PathUtil.getRelativePath(source, p6));
    fs.mkdirs(p7);
    fs.mkdirs(p8);
    createFile(fs, p9);
    expectedValues.put(fs.makeQualified(p9).toString(), PathUtil.getRelativePath(source, p9));

    final URI uri = fs.getUri();
    Path fileSystemPath = new Path(uri.toString());
    source = new Path(fileSystemPath.toString(), source);
    URI target = URI.create("s3://bucket/tmp/target/");
    Path listingPath = new Path(fileSystemPath.toString() + "/" + temporaryRoot + "/META/fileList.seq");
    listing.buildListing(listingPath, options(source, target));
    try (SequenceFile.Reader reader = new SequenceFile.Reader(config, SequenceFile.Reader.file(listingPath))) {
      Text key = new Text();
      CopyListingFileStatus value = new CopyListingFileStatus();
      Map<String, String> actualValues = new HashMap<>();
      while (reader.next(key, value)) {
        if (key.toString().equals("")) {
          // ignore root with empty relPath, which is an entry to be
          // used for preserving root attributes etc.
          continue;
        }
        actualValues.put(value.getPath().toString(), key.toString());
      }

      assertThat(actualValues.size(), is(expectedValues.size()));
      for (Map.Entry<String, String> entry : actualValues.entrySet()) {
        assertThat(entry.getValue(), is(expectedValues.get(entry.getKey())));
      }
    }
  }

  @Test
  public void emptyDirectoriesAreIgnored() throws Exception {
    FileSystem fs = FileSystem.get(config);
    Path source = new Path(temporaryRoot + "/source");
    Path p1 = new Path(source, "1");
    fs.mkdirs(p1);

    final URI uri = fs.getUri();
    Path fileSystemPath = new Path(uri.toString());
    source = new Path(fileSystemPath.toString(), source);
    URI target = URI.create("s3://bucket/tmp/target/");

    Path listingPath = new Path(fileSystemPath.toString() + "/" + temporaryRoot + "/META/fileList.seq");
    
    Configuration conf = new Configuration(config);
    conf.set(SimpleCopyListing.CONF_LABEL_ROOT_PATH, source.toString());
    listing = new SimpleCopyListing(conf, CREDENTIALS);
    listing.buildListing(listingPath, options(p1, target));

    try (SequenceFile.Reader reader = new SequenceFile.Reader(config, SequenceFile.Reader.file(listingPath))) {
      Text key = new Text();
      CopyListingFileStatus value = new CopyListingFileStatus();
      Map<String, String> actualValues = new HashMap<>();
      while (reader.next(key, value)) {
        actualValues.put(value.getPath().toString(), key.toString());
      }

      assertThat(actualValues.size(), is(0));
    }
  }

  @Test(timeout = 10000)
  public void skipFlagFiles() throws Exception {
    FileSystem fs = FileSystem.get(config);
    Path source = new Path(temporaryRoot + "/in4");
    URI target = URI.create("s3://bucket/tmp/out4/");
    createFile(fs, new Path(source, "1/_SUCCESS"));
    createFile(fs, new Path(source, "1/file"));
    createFile(fs, new Path(source, "2"));
    Path listingPath = new Path(temporaryRoot + "/list4");
    listing.buildListing(listingPath, options(source, target));
    assertThat(listing.getNumberOfPaths(), is(2L));
    try (SequenceFile.Reader reader = new SequenceFile.Reader(config, SequenceFile.Reader.file(listingPath))) {
      CopyListingFileStatus fileStatus = new CopyListingFileStatus();
      Text relativePath = new Text();
      assertThat(reader.next(relativePath, fileStatus), is(true));
      assertThat(relativePath.toString(), is("/1/file"));
      assertThat(reader.next(relativePath, fileStatus), is(true));
      assertThat(relativePath.toString(), is("/2"));
      assertThat(reader.next(relativePath, fileStatus), is(false));
    }
  }

  @Test(expected = InvalidInputException.class)
  public void glob() throws Exception {
    FileSystem fs = FileSystem.get(config);
    Path source = new Path(temporaryRoot + "/in/*/*");
    createFile(fs, temporaryRoot + "/in/src1/1.txt");
    createFile(fs, temporaryRoot + "/in/src2/1.txt");
    URI target = URI.create("s3://bucket/tmp/out");
    Path listingFile = new Path(temporaryRoot + "/list");
    listing.buildListing(listingFile, options(source, target));
  }

  @Test(expected = InvalidInputException.class)
  public void copyDirToFile() throws Exception {
    FileSystem fs = FileSystem.get(config);
    Path source = new Path(temporaryRoot + "/in/");
    createFile(fs, new Path(source, "1.txt"));
    createFile(fs, new Path(source, "2.txt"));
    URI target = URI.create("s3://bucket/tmp/out");
    Path listingFile = new Path(temporaryRoot + "/list");
    listing.buildListing(listingFile, options(source, target));
  }

  @Test(timeout = 10000)
  public void numberOfPathsAndNumberOfBytes() throws Exception {
    FileSystem fs = FileSystem.get(config);
    Path source = new Path(temporaryRoot + "/in");
    Path p1 = new Path(source, "1");
    Path p2 = new Path(source, "2");
    URI target = URI.create("s3://bucket/tmp/out/");
    createFile(fs, p1);
    createFile(fs, p2);
    OutputStream out = fs.create(p1);
    out.write("ABC".getBytes());
    out.close();

    out = fs.create(p2);
    out.write("DEF".getBytes());
    out.close();

    Path listingFile = new Path(temporaryRoot + "/file");

    listing.buildListing(listingFile, options(source, target));
    assertThat(listing.getBytesToCopy(), is(6L));
    assertThat(listing.getNumberOfPaths(), is(2L));
  }

  @Test(timeout = 10000)
  public void invalidInput() throws Exception {
    Path source = new Path(temporaryRoot + "/path/does/not/exist");
    URI target = URI.create("s3://bucket/tmp/out");
    Path listingFile = new Path(temporaryRoot + "/file");
    try {
      listing.buildListing(listingFile, options(source, target));
      fail("Invalid input not detected");
    } catch (InvalidInputException ignore) {}
  }

  @Test(timeout = 10000)
  public void buildListingForSingleFile() throws Exception {
    FileSystem fs = FileSystem.get(config);
    String testRootString = temporaryRoot + "/source";
    Path testRoot = new Path(testRootString);
    if (fs.exists(testRoot)) {
      delete(fs, testRootString);
    }

    Path sourceFile = new Path(testRoot, "foo/bar/source.txt");
    Path decoyFile = new Path(testRoot, "target/mooc");
    URI targetFile = URI.create("s3://bucket/target/moo/target.txt");

    createFile(fs, sourceFile.toString());
    createFile(fs, decoyFile.toString());

    final Path listFile = new Path(testRoot, temporaryRoot + "/fileList.seq");

    listing.buildListing(listFile, options(sourceFile, targetFile));

    try (SequenceFile.Reader reader = new SequenceFile.Reader(config, SequenceFile.Reader.file(listFile))) {
      CopyListingFileStatus fileStatus = new CopyListingFileStatus();
      Text relativePath = new Text();
      assertThat(reader.next(relativePath, fileStatus), is(true));
      assertThat(relativePath.toString(), is("/source.txt"));
    }
  }

  @Test(timeout = 10000)
  public void buildListingForMultipleSources() throws Exception {
    FileSystem fs = FileSystem.get(config);
    String testRootString = temporaryRoot + "/source";
    Path testRoot = new Path(testRootString);
    if (fs.exists(testRoot)) {
      delete(fs, testRootString);
    }

    Path sourceDir1 = new Path(testRoot, "foo/baz/");
    Path sourceDir2 = new Path(testRoot, "foo/bang/");
    Path sourceFile1 = new Path(testRoot, "foo/bar/source.txt");
    URI target = URI.create("s3://bucket/target/moo/");

    fs.mkdirs(sourceDir1);
    fs.mkdirs(sourceDir2);
    createFile(fs, new Path(sourceDir1, "baz_1.dat"));
    createFile(fs, new Path(sourceDir1, "baz_2.dat"));
    createFile(fs, new Path(sourceDir2, "bang_0.dat"));
    createFile(fs, sourceFile1.toString());

    final Path listFile = new Path(testRoot, temporaryRoot + "/fileList.seq");

    listing.buildListing(listFile, options(Arrays.asList(sourceFile1, sourceDir1, sourceDir2), target));

    try (SequenceFile.Reader reader = new SequenceFile.Reader(config, SequenceFile.Reader.file(listFile))) {
      CopyListingFileStatus fileStatus = new CopyListingFileStatus();
      Text relativePath = new Text();
      assertThat(reader.next(relativePath, fileStatus), is(true));
      assertThat(relativePath.toString(), is("/source.txt"));
      assertThat(reader.next(relativePath, fileStatus), is(true));
      assertThat(relativePath.toString(), is("/baz_1.dat"));
      assertThat(reader.next(relativePath, fileStatus), is(true));
      assertThat(relativePath.toString(), is("/baz_2.dat"));
      assertThat(reader.next(relativePath, fileStatus), is(true));
      assertThat(relativePath.toString(), is("/bang_0.dat"));
    }
  }

  @Test (timeout = 10000)
  public void buildListingForMultipleSourcesWithRootPath() throws Exception {
    String testRootString = temporaryRoot + "/source";
    Configuration conf = new Configuration(config);
    conf.set(SimpleCopyListing.CONF_LABEL_ROOT_PATH, testRootString);
    listing = new SimpleCopyListing(conf, CREDENTIALS);

    FileSystem fs = FileSystem.get(config);
    Path testRoot = new Path(testRootString);
    if (fs.exists(testRoot)) {
      delete(fs, testRootString);
    }

    Path sourceDir1 = new Path(testRoot, "foo/baz/");
    Path sourceDir2 = new Path(testRoot, "foo/bang/");
    Path sourceFile1 = new Path(testRoot, "foo/bar/source.txt");
    URI target = URI.create("s3://bucket/target/moo/");

    fs.mkdirs(sourceDir1);
    fs.mkdirs(sourceDir2);
    createFile(fs, new Path(sourceDir1, "0.dat"));
    createFile(fs, new Path(sourceDir1, "1.dat"));
    createFile(fs, new Path(sourceDir2, "0.dat"));
    createFile(fs, sourceFile1.toString());

    final Path listFile = new Path(testRoot, temporaryRoot + "/fileList.seq");

    listing.buildListing(listFile, options(Arrays.asList(sourceFile1, sourceDir1, sourceDir2), target));

    try (SequenceFile.Reader reader = new SequenceFile.Reader(config, SequenceFile.Reader.file(listFile))) {
      CopyListingFileStatus fileStatus = new CopyListingFileStatus();
      Text relativePath = new Text();
      assertThat(reader.next(relativePath, fileStatus), is(true));
      assertThat(relativePath.toString(), is("/foo/bar/source.txt"));
      assertThat(reader.next(relativePath, fileStatus), is(true));
      assertThat(relativePath.toString(), is("/foo/baz/0.dat"));
      assertThat(reader.next(relativePath, fileStatus), is(true));
      assertThat(relativePath.toString(), is("/foo/baz/1.dat"));
      assertThat(reader.next(relativePath, fileStatus), is(true));
      assertThat(relativePath.toString(), is("/foo/bang/0.dat"));
    }
  }

  @Test(expected = DuplicateFileException.class)
  public void failOnDuplicateFile() throws Exception {
    FileSystem fs = FileSystem.get(config);
    String testRootString = temporaryRoot + "/source";
    Path testRoot = new Path(testRootString);
    if (fs.exists(testRoot)) {
      delete(fs, testRootString);
    }

    Path sourceDir1 = new Path(testRoot, "foo/bar/");
    Path sourceDir2 = new Path(testRoot, "foo/baz/");
    URI target = URI.create("s3://bucket/target/moo/");

    fs.mkdirs(sourceDir1);
    fs.mkdirs(sourceDir2);
    createFile(fs, new Path(sourceDir1, "1.dat"));
    createFile(fs, new Path(sourceDir2, "1.dat"));

    final Path listFile = new Path(testRoot, temporaryRoot + "/fileList.seq");

    listing.buildListing(listFile, options(Arrays.asList(sourceDir1, sourceDir2), target));
  }

  @Test
  public void failOnCloseError() throws IOException {
    File inFile = File.createTempFile("TestCopyListingIn", null);
    inFile.deleteOnExit();
    File outFile = File.createTempFile("TestCopyListingOut", null);
    outFile.deleteOnExit();
    Path source = new Path(inFile.toURI());

    Exception expectedEx = new IOException("boom");
    SequenceFile.Writer writer = mock(SequenceFile.Writer.class);
    doThrow(expectedEx).when(writer).close();

    Exception actualEx = null;
    try {
      listing.doBuildListing(writer, options(source, outFile.toURI()));
    } catch (Exception e) {
      actualEx = e;
    }
    Assert.assertNotNull("close writer didn't fail", actualEx);
    Assert.assertEquals(expectedEx, actualEx);
  }

  private S3MapReduceCpOptions options(Path source, URI target) {
    return options(Arrays.asList(source), target);
  }

  private S3MapReduceCpOptions options(List<Path> sources, URI target) {
    return S3MapReduceCpOptions.builder(sources, target).build();
  }

}
