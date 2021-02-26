/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.TestCopyListing} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/TestCopyListing.java
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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.junit.Test;

public class CopyListingTest {

  private static final Credentials CREDENTIALS = new Credentials();
  private static final Configuration CONFIG = new Configuration();

  @Test(timeout = 10000)
  public void defaultCopyListing() throws Exception {
    S3MapReduceCpOptions options = S3MapReduceCpOptions
        .builder(Arrays.asList(new Path("/tmp/in4")), new URI("/tmp/out4"))
        .build();
    CopyListing listing = CopyListing.getCopyListing(CONFIG, CREDENTIALS, options);
    assertThat(listing, is(instanceOf(SimpleCopyListing.class)));
  }

}
