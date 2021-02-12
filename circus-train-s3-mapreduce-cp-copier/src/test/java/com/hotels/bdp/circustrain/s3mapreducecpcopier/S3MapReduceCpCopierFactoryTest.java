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
package com.hotels.bdp.circustrain.s3mapreducecpcopier;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.codahale.metrics.MetricRegistry;

import com.hotels.bdp.circustrain.api.copier.CopierOptions;

@RunWith(MockitoJUnitRunner.class)
public class S3MapReduceCpCopierFactoryTest {

  @Mock
  private Configuration conf;
  @Mock
  private CopierOptions copierOptions;
  @Mock
  private MetricRegistry runningMetricsRegistry;

  @Test
  public void supportsSchemes() throws Exception {
    S3MapReduceCpCopierFactory factory = new S3MapReduceCpCopierFactory(conf, runningMetricsRegistry);
    assertTrue(factory.supportsSchemes("hdfs", "s3"));
    assertTrue(factory.supportsSchemes("hdfs", "s3a"));
    assertTrue(factory.supportsSchemes("hdfs", "s3n"));
    assertTrue(factory.supportsSchemes("file", "s3"));
    assertTrue(factory.supportsSchemes("whatever", "s3"));
  }

  @Test
  public void doesNotsupportsSchemes() throws Exception {
    S3MapReduceCpCopierFactory factory = new S3MapReduceCpCopierFactory(conf, runningMetricsRegistry);
    assertFalse(factory.supportsSchemes("hdfs", "hdfs"));
    assertFalse(factory.supportsSchemes("s3", "s3"));
    assertFalse(factory.supportsSchemes("s3", "hdfs"));
    assertFalse(factory.supportsSchemes("hdfs", "s321"));
  }

}
