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
package com.hotels.bdp.circustrain.s3s3copier;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;

import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.s3s3copier.aws.AmazonS3ClientFactory;
import com.hotels.bdp.circustrain.s3s3copier.aws.ListObjectsRequestFactory;
import com.hotels.bdp.circustrain.s3s3copier.aws.TransferManagerFactory;

@RunWith(MockitoJUnitRunner.class)
public class S3S3CopierFactoryTest {

  private @Mock AmazonS3ClientFactory clientFactory;
  private @Mock ListObjectsRequestFactory listObjectsRequestFactory;
  private @Mock TransferManagerFactory transferManagerFactory;
  private @Mock MetricRegistry metricsRegistry;

  private S3S3CopierFactory factory;

  @Before
  public void setUp() {
    factory = new S3S3CopierFactory(clientFactory, listObjectsRequestFactory, transferManagerFactory, metricsRegistry);
  }

  @Test
  public void supportsSchemes() throws Exception {
    assertTrue(factory.supportsSchemes("s3", "s3"));
    assertTrue(factory.supportsSchemes("s3a", "s3a"));
    assertTrue(factory.supportsSchemes("s3n", "s3n"));
    assertTrue(factory.supportsSchemes("s3", "s3n"));
  }

  @Test
  public void unsupportedSchemes() throws Exception {
    assertFalse(factory.supportsSchemes("s3", "s345"));
    assertFalse(factory.supportsSchemes("s345", "s3"));
    assertFalse(factory.supportsSchemes("hdfs", "s3"));
    assertFalse(factory.supportsSchemes("s3", "hdfs"));
    assertFalse(factory.supportsSchemes(null, null));
    assertFalse(factory.supportsSchemes("", ""));
  }

  @Test
  public void newInstance() throws Exception {
    String eventId = "eventID";
    Path sourceBaseLocation = new Path("source");
    Path replicaLocation = new Path("replica");
    Map<String, Object> copierOptions = new HashMap<>();
    Copier copier = factory.newInstance(eventId, sourceBaseLocation, replicaLocation, copierOptions);
    assertNotNull(copier);
  }

  @Test
  public void newInstancePartitions() throws Exception {
    String eventId = "eventID";
    Path sourceBaseLocation = new Path("source");
    Path replicaLocation = new Path("replica");
    Map<String, Object> copierOptions = new HashMap<>();
    List<Path> subLocations = Lists.newArrayList(new Path(sourceBaseLocation, "sub"));
    Copier copier = factory.newInstance(eventId, sourceBaseLocation, subLocations, replicaLocation, copierOptions);
    assertNotNull(copier);
  }
}
