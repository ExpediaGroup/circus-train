/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import static com.hotels.bdp.circustrain.distcpcopier.DistCpOptionsParser.ATOMIC_COMMIT;
import static com.hotels.bdp.circustrain.distcpcopier.DistCpOptionsParser.ATOMIC_WORK_PATH;
import static com.hotels.bdp.circustrain.distcpcopier.DistCpOptionsParser.COPY_STRATEGY;
import static com.hotels.bdp.circustrain.distcpcopier.DistCpOptionsParser.FILE_ATTRIBUTES;
import static com.hotels.bdp.circustrain.distcpcopier.DistCpOptionsParser.IGNORE_FAILURES;
import static com.hotels.bdp.circustrain.distcpcopier.DistCpOptionsParser.LOG_PATH;
import static com.hotels.bdp.circustrain.distcpcopier.DistCpOptionsParser.MAX_MAPS;
import static com.hotels.bdp.circustrain.distcpcopier.DistCpOptionsParser.PRESERVE_RAW_XATTRS;
import static com.hotels.bdp.circustrain.distcpcopier.DistCpOptionsParser.SKIP_CRC;
import static com.hotels.bdp.circustrain.distcpcopier.DistCpOptionsParser.SSL_CONFIGURATION_FILE;
import static com.hotels.bdp.circustrain.distcpcopier.DistCpOptionsParser.TASK_BANDWIDTH;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.junit.Test;


public class DistCpOptionsParserTest {

  private final DistCpOptionsParser parser = new DistCpOptionsParser(Arrays.asList(new Path("sourc-path")),
      new Path("replication-path"));

  private Map<String, Object> defaultOptions() {
    Map<String, Object> options = new HashMap<>();
    options.put(FILE_ATTRIBUTES, null);
    options.put(PRESERVE_RAW_XATTRS, null);
    options.put(ATOMIC_COMMIT, null);
    options.put(ATOMIC_WORK_PATH, null);
    options.put(COPY_STRATEGY, null);
    options.put(IGNORE_FAILURES, null);
    options.put(LOG_PATH, null);
    options.put(TASK_BANDWIDTH, null);
    options.put(MAX_MAPS, null);
    options.put(SKIP_CRC, null);
    options.put(SSL_CONFIGURATION_FILE, null);
    return options;
  }

  private void assertDefaultValues(DistCpOptions distCpOptions) {
    assertThat(distCpOptions, is(not(nullValue())));
    assertThat(distCpOptions.preserveAttributes().hasNext(), is(false));
    assertThat(distCpOptions.shouldPreserveRawXattrs(), is(false));
    assertThat(distCpOptions.shouldAppend(), is(false));
    assertThat(distCpOptions.shouldAtomicCommit(), is(false));
    assertThat(distCpOptions.getAtomicWorkPath(), is(nullValue()));
    assertThat(distCpOptions.shouldBlock(), is(true));
    assertThat(distCpOptions.getCopyStrategy(), is(DistCpConstants.UNIFORMSIZE));
    assertThat(distCpOptions.shouldDeleteMissing(), is(false));
    assertThat(distCpOptions.shouldIgnoreFailures(), is(false));
    assertThat(distCpOptions.getLogPath(), is(nullValue()));
    assertThat(distCpOptions.getMapBandwidth(), is(DistCpConstants.DEFAULT_BANDWIDTH_MB));
    assertThat(distCpOptions.getMaxMaps(), is(DistCpConstants.DEFAULT_MAPS));
    assertThat(distCpOptions.shouldOverwrite(), is(false));
    assertThat(distCpOptions.shouldSkipCRC(), is(false));
    assertThat(distCpOptions.getSslConfigurationFile(), is(nullValue()));
    assertThat(distCpOptions.shouldSyncFolder(), is(false));
    assertThat(distCpOptions.getTargetPathExists(), is(true));
  }

  @Test
  public void defaults() {
    assertDefaultValues(parser.parse(defaultOptions()));
  }

  @Test
  public void nullCopierOptions() {
    assertDefaultValues(parser.parse(null));
  }

  @Test
  public void emptyCopierOptions() {
    assertDefaultValues(parser.parse(Collections.<String, Object> emptyMap()));
  }

  @Test
  public void typical() {
    Map<String, Object> options = defaultOptions();
    options.put(FILE_ATTRIBUTES, Arrays.asList("replication", "blocksize", "user", "group", "permission",
        "checksumtype", "acl", "xattr", "times"));
    options.put(PRESERVE_RAW_XATTRS, "true");
    options.put(ATOMIC_COMMIT, "false");
    options.put(ATOMIC_WORK_PATH, "atomic-work-path");
    options.put(COPY_STRATEGY, "copy-strategy");
    options.put(IGNORE_FAILURES, "true");
    options.put(LOG_PATH, "log-path");
    options.put(TASK_BANDWIDTH, "500");
    options.put(MAX_MAPS, "2");
    options.put(SKIP_CRC, "false");
    options.put(SSL_CONFIGURATION_FILE, "ssl-configuration-file");
    DistCpOptions distCpOptions = parser.parse(options);
    for (FileAttribute attribute : FileAttribute.values()) {
      assertThat(distCpOptions.shouldPreserve(attribute), is(true));
    }
    assertThat(distCpOptions.shouldPreserveRawXattrs(), is(true));
    assertThat(distCpOptions.shouldAppend(), is(false));
    assertThat(distCpOptions.shouldAtomicCommit(), is(false));
    assertThat(distCpOptions.getAtomicWorkPath(), is(new Path("atomic-work-path")));
    assertThat(distCpOptions.shouldBlock(), is(true));
    assertThat(distCpOptions.getCopyStrategy(), is("copy-strategy"));
    assertThat(distCpOptions.shouldDeleteMissing(), is(false));
    assertThat(distCpOptions.shouldIgnoreFailures(), is(true));
    assertThat(distCpOptions.getLogPath(), is(new Path("log-path")));
    assertThat(distCpOptions.getMapBandwidth(), is(500));
    assertThat(distCpOptions.getMaxMaps(), is(2));
    assertThat(distCpOptions.shouldOverwrite(), is(false));
    assertThat(distCpOptions.shouldSkipCRC(), is(false));
    assertThat(distCpOptions.getSslConfigurationFile(), is("ssl-configuration-file"));
    assertThat(distCpOptions.shouldSyncFolder(), is(false));
    assertThat(distCpOptions.getTargetPathExists(), is(true));
  }

  @Test(expected = IllegalArgumentException.class)
  public void zeroMapBandwidth() {
    Map<String, Object> options = defaultOptions();
    options.put(TASK_BANDWIDTH, "0");
    parser.parse(options);
    options.put(MAX_MAPS, "2");
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeMapBandwidth() {
    Map<String, Object> options = defaultOptions();
    options.put(TASK_BANDWIDTH, "-1");
    parser.parse(options);
  }

  @Test(expected = IllegalArgumentException.class)
  public void zeroMaxMaps() {
    Map<String, Object> options = defaultOptions();
    options.put(MAX_MAPS, "0");
    parser.parse(options);
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeMaxMaps() {
    Map<String, Object> options = defaultOptions();
    options.put(MAX_MAPS, "-1");
    parser.parse(options);
  }

}
