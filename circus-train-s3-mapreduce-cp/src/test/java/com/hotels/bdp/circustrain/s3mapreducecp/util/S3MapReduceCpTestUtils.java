/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.util.DistCpTestUtils} and {@code org.apache.hadoop.tools.util.TestDistCpUtils} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/util/DistCpTestUtils.java
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/util/TestDistCpUtils.java
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
package com.hotels.bdp.circustrain.s3mapreducecp.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;

import com.hotels.bdp.circustrain.s3mapreducecp.S3MapReduceCp;

/**
 * Utility class for DistCpTests
 */
public class S3MapReduceCpTestUtils {
  private static final Log LOG = LogFactory.getLog(ConfigurationUtilTest.class);

  public static MiniDFSCluster.Builder newMiniClusterBuilder(Configuration config) throws IOException {
    String buildDirectory = System.getProperty("project.build.directory", "target");
    buildDirectory += "/minicluster/test/data";
    System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, buildDirectory);
    return new MiniDFSCluster.Builder(config);
  }

  /**
   * Asserts the XAttrs returned by getXAttrs for a specific path match an expected set of XAttrs.
   *
   * @param path String path to check
   * @param fs FileSystem to use for the path
   * @param expectedXAttrs XAttr[] expected xAttrs
   * @throws Exception if there is any error
   */
  public static void assertXAttrs(Path path, FileSystem fs, Map<String, byte[]> expectedXAttrs) throws Exception {
    Map<String, byte[]> xAttrs = fs.getXAttrs(path);
    assertEquals(path.toString(), expectedXAttrs.size(), xAttrs.size());
    Iterator<Entry<String, byte[]>> i = expectedXAttrs.entrySet().iterator();
    while (i.hasNext()) {
      Entry<String, byte[]> e = i.next();
      String name = e.getKey();
      byte[] value = e.getValue();
      if (value == null) {
        assertTrue(xAttrs.containsKey(name) && xAttrs.get(name) == null);
      } else {
        assertArrayEquals(value, xAttrs.get(name));
      }
    }
  }

  /**
   * Runs distcp from src to dst, preserving XAttrs. Asserts the expected exit code.
   *
   * @param exitCode expected exit code
   * @param src distcp src path
   * @param dst distcp destination
   * @param options distcp command line options
   * @param conf Configuration to use
   * @throws Exception if there is any error
   */
  public static void assertRunDistCp(int exitCode, String src, String dst, String options, Configuration conf)
    throws Exception {
    S3MapReduceCp distCp = new S3MapReduceCp(conf, null);
    String[] optsArr = options == null ? new String[] { src, dst } : new String[] { options, src, dst };
    assertEquals(exitCode, ToolRunner.run(conf, distCp, optsArr));
  }

  private static Random rand = new Random();

  public static String createTestSetup(FileSystem fs) throws IOException {
    return createTestSetup("/tmp1", fs, FsPermission.getDefault());
  }

  public static String createTestSetup(FileSystem fs, FsPermission perm) throws IOException {
    return createTestSetup("/tmp1", fs, perm);
  }

  public static String createTestSetup(String baseDir, FileSystem fs, FsPermission perm) throws IOException {
    String base = getBase(baseDir);
    fs.mkdirs(new Path(base + "/newTest/hello/world1"));
    fs.mkdirs(new Path(base + "/newTest/hello/world2/newworld"));
    fs.mkdirs(new Path(base + "/newTest/hello/world3/oldworld"));
    fs.setPermission(new Path(base + "/newTest"), perm);
    fs.setPermission(new Path(base + "/newTest/hello"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world1"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world2"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world2/newworld"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world3"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world3/oldworld"), perm);
    createFile(fs, new Path(base, "/newTest/1"));
    createFile(fs, new Path(base, "/newTest/hello/2"));
    createFile(fs, new Path(base, "/newTest/hello/world3/oldworld/3"));
    createFile(fs, new Path(base, "/newTest/hello/world2/4"));
    return base;
  }

  private static String getBase(String base) {
    String location = String.valueOf(rand.nextLong());
    return base + "/" + location;
  }

  public static void delete(FileSystem fs, String path) {
    try {
      if (fs != null) {
        if (path != null) {
          fs.delete(new Path(path), true);
        }
      }
    } catch (IOException e) {
      LOG.warn("Exception encountered ", e);
    }
  }

  public static void createFile(FileSystem fs, String filePath) throws IOException {
    Path path = new Path(filePath);
    createFile(fs, path);
  }

  /** Creates a new, empty file at filePath and always overwrites */
  public static void createFile(FileSystem fs, Path filePath) throws IOException {
    OutputStream out = fs.create(filePath, true);
    IOUtils.closeStream(out);
  }

  /** Creates a new, empty directory at dirPath and always overwrites */
  public static void createDirectory(FileSystem fs, Path dirPath) throws IOException {
    fs.delete(dirPath, true);
    boolean created = fs.mkdirs(dirPath);
    if (!created) {
      LOG.warn("Could not create directory " + dirPath + " this might cause test failures.");
    }
  }

  public static boolean checkIfFoldersAreInSync(FileSystem fs, String targetBase, String sourceBase)
    throws IOException {
    Path base = new Path(targetBase);

    Stack<Path> stack = new Stack<>();
    stack.push(base);
    while (!stack.isEmpty()) {
      Path file = stack.pop();
      if (!fs.exists(file)) {
        continue;
      }
      FileStatus[] fStatus = fs.listStatus(file);
      if (fStatus == null || fStatus.length == 0) {
        continue;
      }

      for (FileStatus status : fStatus) {
        if (status.isDirectory()) {
          stack.push(status.getPath());
        }
        assertTrue(
            fs.exists(new Path(sourceBase + "/" + PathUtil.getRelativePath(new Path(targetBase), status.getPath()))));
      }
    }
    return true;
  }

}
