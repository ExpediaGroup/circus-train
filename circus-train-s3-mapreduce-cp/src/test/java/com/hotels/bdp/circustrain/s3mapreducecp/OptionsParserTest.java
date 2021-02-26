/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.TestOptionsParser} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/TestOptionsParser.java
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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.StorageClass;

public class OptionsParserTest {

  private OptionsParser parser = new OptionsParser();

  @Test
  public void typical() {
    S3MapReduceCpOptions options = parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest",
        "hdfs://localhost:8020/target/");
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(Arrays.asList(new Path("hdfs://localhost:8020/source/first"))));
    assertThat(options.getTarget(), is(URI.create("hdfs://localhost:8020/target/")));
    assertThat(options.getCredentialsProvider(), is(nullValue()));
    assertThat(options.getMultipartUploadPartSize(),
        is(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue()));
    assertThat(options.isS3ServerSideEncryption(),
        is(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue()));
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
    assertThat(options.getNumberOfUploadWorkers(),
        is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
    assertThat(options.getMultipartUploadThreshold(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
  }

  @Test
  public void help() {
    S3MapReduceCpOptions options = parser.parse("--help");
    assertThat(options.isHelp(), is(true));
  }

  @Test
  public void blocking() {
    S3MapReduceCpOptions options = parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest",
        "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks", "--async");
    assertThat(options.isBlocking(), is(false));
  }

  @Test(expected = IllegalArgumentException.class)
  public void missingSourcePath() {
    parser.parse("--dest", "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks");
  }

  @Test(expected = IllegalArgumentException.class)
  public void relativeSourcePath() {
    parser.parse("--src", "./source/first", "--dest", "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks");
  }

  @Test
  public void multipleSourcePaths() {
    S3MapReduceCpOptions options = parser.parse("--src",
        "hdfs://localhost:8020/source/first,hdfs://localhost:8020/source/second,hdfs://localhost:8020/source/third",
        "--dest", "hdfs://localhost:8020/target/");
    assertThat(options.getSources(), is(Arrays.asList(new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/source/second"), new Path("hdfs://localhost:8020/source/third"))));
  }

  @Test(expected = IllegalArgumentException.class)
  public void multipleSourcePathsOneOfWhichIsRelative() {
    parser.parse("--src", "hdfs://localhost:8020/source/first,source/second,hdfs://localhost:8020/source/third",
        "--dest", "hdfs://localhost:8020/target/");
  }

  @Test(expected = IllegalArgumentException.class)
  public void missingTargetPath() {
    parser.parse("--src", "hdfs://localhost:8020/source/first", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks");
  }

  @Test(expected = IllegalArgumentException.class)
  public void relativeTargetPath() {
    parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest", "../target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks");
  }

  @Test
  public void credentialsProvider() {
    S3MapReduceCpOptions options = parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest",
        "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks");
    assertThat(options.getCredentialsProvider(),
        is(URI.create("jceks://hdfs@localhost:8020/security/credentials.jceks")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidCredentialsProvider() {
    parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest", "hdfs://localhost:8020/target/",
        "--credentialsProvider", "abs");
  }

  @Test
  public void multipartUploadPartSize() {
    S3MapReduceCpOptions options = parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest",
        "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks", "--multipartUploadChunkSize", "654321");
    assertThat(options.getMultipartUploadPartSize(), is(654321L));
  }

  @Test(expected = IllegalArgumentException.class)
  public void zeroMultipartUploadPartSize() {
    parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest", "hdfs://localhost:8020/target/",
        "--credentialsProvider", "jceks://hdfs@localhost:8020/security/credentials.jceks", "--multipartUploadChunkSize",
        "0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeMultipartUploadPartSize() {
    parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest", "hdfs://localhost:8020/target/",
        "--credentialsProvider", "jceks://hdfs@localhost:8020/security/credentials.jceks", "--multipartUploadChunkSize",
        "-123");
  }

  @Test
  public void s3ServerSideEncryption() {
    S3MapReduceCpOptions options = new OptionsParser().parse("--src", "hdfs://localhost:8020/source/first", "--dest",
        "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks", "--s3ServerSideEncryption");
    assertThat(options.isS3ServerSideEncryption(), is(true));
  }

  @Test
  public void storageClass() {
    for (StorageClass storageClass : StorageClass.values()) {
      S3MapReduceCpOptions options = new OptionsParser().parse("--src", "hdfs://localhost:8020/source/first", "--dest",
          "hdfs://localhost:8020/target/", "--credentialsProvider",
          "jceks://hdfs@localhost:8020/security/credentials.jceks", "--storageClass", storageClass.toString());
      assertThat(options.getStorageClass(), is(storageClass.toString()));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidStorageClass() {
    parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest", "hdfs://localhost:8020/target/",
        "--credentialsProvider", "jceks://hdfs@localhost:8020/security/credentials.jceks", "--storageClass", "pie");
  }

  @Test
  public void maxBandwidth() {
    S3MapReduceCpOptions options = parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest",
        "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks", "--maxBandwidth", "123456");
    assertThat(options.getMaxBandwidth(), is(123456L));
  }

  @Test(expected = IllegalArgumentException.class)
  public void zeroMaxBandwidth() {
    parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest", "hdfs://localhost:8020/target/",
        "--credentialsProvider", "jceks://hdfs@localhost:8020/security/credentials.jceks", "--maxBandwidth", "0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeMaxBandwidth() {
    parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest", "hdfs://localhost:8020/target/",
        "--credentialsProvider", "jceks://hdfs@localhost:8020/security/credentials.jceks", "--maxBandwidth", "-123");
  }

  @Test
  public void numberOfUploadWorkers() {
    S3MapReduceCpOptions options = parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest",
        "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks", "--numberOfUploadWorkers", "33");
    assertThat(options.getNumberOfUploadWorkers(), is(33));
  }

  @Test(expected = IllegalArgumentException.class)
  public void zeroNumberOfUploadWorkers() {
    parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest", "hdfs://localhost:8020/target/",
        "--credentialsProvider", "jceks://hdfs@localhost:8020/security/credentials.jceks", "--numberOfUploadWorkers",
        "0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeNumberOfUploadWorkers() {
    parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest", "hdfs://localhost:8020/target/",
        "--credentialsProvider", "jceks://hdfs@localhost:8020/security/credentials.jceks", "--numberOfUploadWorkers",
        "-1");
  }

  @Test
  public void multipartUploadThreshold() {
    S3MapReduceCpOptions options = parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest",
        "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks", "--multipartUploadThreshold", "48");
    assertThat(options.getMultipartUploadThreshold(), is(48L));
  }

  @Test(expected = IllegalArgumentException.class)
  public void zeroMultipartUploadThreshold() {
    parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest", "hdfs://localhost:8020/target/",
        "--credentialsProvider", "jceks://hdfs@localhost:8020/security/credentials.jceks", "--multipartUploadThreshold",
        "0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeMultipartUploadThreshold() {
    parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest", "hdfs://localhost:8020/target/",
        "--credentialsProvider", "jceks://hdfs@localhost:8020/security/credentials.jceks", "--multipartUploadThreshold",
        "-1");
  }

  @Test
  public void maxMaps() {
    S3MapReduceCpOptions options = parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest",
        "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks", "--maxMaps", "15");
    assertThat(options.getMaxMaps(), is(15));
  }

  @Test
  public void zeroMaxMaps() {
    S3MapReduceCpOptions options = parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest",
        "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks", "--maxMaps", "0");
    assertThat(options.getMaxMaps(), is(1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeMaxMaps() {
    parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest", "hdfs://localhost:8020/target/",
        "--credentialsProvider", "jceks://hdfs@localhost:8020/security/credentials.jceks", "--maxMaps", "-1");
  }

  @Test
  public void copyStrategy() {
    S3MapReduceCpOptions options = parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest",
        "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks", "--copyStrategy", "mycopystrategy");
    assertThat(options.getCopyStrategy(), is("mycopystrategy"));
  }

  @Test
  public void logPath() {
    S3MapReduceCpOptions options = parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest",
        "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks", "--logPath", "hdfs://localhost:8020/logs/");
    assertThat(options.getLogPath(), is(new Path("hdfs://localhost:8020/logs/")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void relativeLogPath() {
    parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest", "hdfs://localhost:8020/target/", "--logPath",
        "logs");
  }

  @Test
  public void regions() {
    for (Regions region : Regions.values()) {
      S3MapReduceCpOptions options = new OptionsParser().parse("--src", "hdfs://localhost:8020/source/first", "--dest",
          "hdfs://localhost:8020/target/", "--credentialsProvider",
          "jceks://hdfs@localhost:8020/security/credentials.jceks", "--region", region.getName());
      assertThat(options.getRegion(), is(region.getName()));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidRegion() {
    parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest", "hdfs://localhost:8020/target/",
        "--credentialsProvider", "jceks://hdfs@localhost:8020/security/credentials.jceks", "--region", "cake");
  }

  @Test
  public void ignoreFailures() {
    S3MapReduceCpOptions options = parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest",
        "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks", "--ignoreFailures");
    assertThat(options.isIgnoreFailures(), is(true));
  }

  @Test
  public void uploadRetryCount() {
    S3MapReduceCpOptions options = parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest",
        "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks", "--uploadRetryCount", "13");
    assertThat(options.getUploadRetryCount(), is(13));
  }

  @Test
  public void zeroUploadRetryCount() {
    S3MapReduceCpOptions options = parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest",
        "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks", "--uploadRetryCount", "0");
    assertThat(options.getUploadRetryCount(), is(0));
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeUploadRetryCount() {
    parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest", "hdfs://localhost:8020/target/",
        "--credentialsProvider", "jceks://hdfs@localhost:8020/security/credentials.jceks", "--uploadRetryCount", "-1");
  }

  @Test
  public void uploadRetryDelayMs() {
    S3MapReduceCpOptions options = parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest",
        "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks", "--uploadRetryDelayMs", "500");
    assertThat(options.getUploadRetryDelayMs(), is(500L));
  }

  @Test
  public void zeroUploadRetryDelayMs() {
    S3MapReduceCpOptions options = parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest",
        "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks", "--uploadRetryDelayMs", "0");
    assertThat(options.getUploadRetryDelayMs(), is(0L));
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeUploadRetryDelayMs() {
    parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest", "hdfs://localhost:8020/target/",
        "--credentialsProvider", "jceks://hdfs@localhost:8020/security/credentials.jceks", "--uploadRetryDelayMs",
        "-1");
  }

  @Test
  public void uploadBufferSize() {
    S3MapReduceCpOptions options = parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest",
        "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks", "--uploadBufferSize", "4096");
    assertThat(options.getUploadBufferSize(), is(4096));
  }

  @Test
  public void zeroUploadBufferSize() {
    S3MapReduceCpOptions options = parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest",
        "hdfs://localhost:8020/target/", "--credentialsProvider",
        "jceks://hdfs@localhost:8020/security/credentials.jceks", "--uploadBufferSize", "0");
    assertThat(options.getUploadBufferSize(), is(0));
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeUploadBufferSize() {
    parser.parse("--src", "hdfs://localhost:8020/source/first", "--dest", "hdfs://localhost:8020/target/",
        "--credentialsProvider", "jceks://hdfs@localhost:8020/security/credentials.jceks", "--uploadBufferSize", "-1");
  }

}
