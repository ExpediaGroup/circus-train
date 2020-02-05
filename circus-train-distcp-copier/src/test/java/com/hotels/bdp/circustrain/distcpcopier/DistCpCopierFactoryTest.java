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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;

import com.hotels.bdp.circustrain.api.copier.Copier;

public class DistCpCopierFactoryTest {

  private final Map<String, Object> copierOptions = new HashMap<>();
  private final Configuration conf = new Configuration();
  private final MetricRegistry metricRegistry = new MetricRegistry();

  @Test
  public void supportSchemes() {
    DistCpCopierFactory factory = new DistCpCopierFactory(conf, metricRegistry);
    assertThat(factory.supportsSchemes("hdfs", "s3a"), is(true));
    assertThat(factory.supportsSchemes("hdfs", "s3"), is(true));
    assertThat(factory.supportsSchemes("hdfs", "s3n"), is(true));
    assertThat(factory.supportsSchemes("hdfs", "hdfs"), is(true));
    assertThat(factory.supportsSchemes("hdfs", "file"), is(true));
    assertThat(factory.supportsSchemes("hdfs", "other"), is(true));
  }

  @Test
  public void hdfsTableCopier() {
    DistCpCopierFactory factory = new DistCpCopierFactory(conf, metricRegistry);
    Copier copier = factory.newInstance("evt-123", new Path("confLocation"), Collections.<Path> emptyList(),
        new Path("hdfs:/replicaLocation"), copierOptions);
    assertEquals(DistCpCopier.class, copier.getClass());
  }

  @Test
  public void fileTableCopier() {
    DistCpCopierFactory factory = new DistCpCopierFactory(conf, metricRegistry);
    Copier copier = factory.newInstance("evt-123", new Path("confLocation"), Collections.<Path> emptyList(),
        new Path("file:/replicaLocation"), copierOptions);
    assertEquals(DistCpCopier.class, copier.getClass());
  }

  @Test
  public void s3aTableCopier() {
    DistCpCopierFactory factory = new DistCpCopierFactory(conf, metricRegistry);
    Copier copier = factory.newInstance("evt-123", new Path("confLocation"), Collections.<Path> emptyList(),
        new Path("s3a:/replicaLocation"), copierOptions);
    assertEquals(DistCpCopier.class, copier.getClass());
  }

  @Test
  public void s3TableCopier() {
    DistCpCopierFactory factory = new DistCpCopierFactory(conf, metricRegistry);
    Copier copier = factory.newInstance("evt-123", new Path("confLocation"), Collections.<Path> emptyList(),
        new Path("s3:/replicaLocation"), copierOptions);
    assertEquals(DistCpCopier.class, copier.getClass());
  }

  @Test
  public void s3nTableCopier() {
    DistCpCopierFactory factory = new DistCpCopierFactory(conf, metricRegistry);
    Copier copier = factory.newInstance("evt-123", new Path("confLocation"), Collections.<Path> emptyList(),
        new Path("s3n:/replicaLocation"), copierOptions);
    assertEquals(DistCpCopier.class, copier.getClass());
  }

}
