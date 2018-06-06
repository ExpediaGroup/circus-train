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
package com.hotels.bdp.circustrain.avro.transformation;

import static org.junit.Assert.*;

import org.junit.Test;

import com.hotels.bdp.circustrain.avro.conf.AvroSerDeConfig;

public class AbstractAvroSerDeTransformationTest {

  @Test
  public void constructSourceWithScheme() {
    AvroSerDeConfig config = new AvroSerDeConfig();
    config.setSourceAvroUrlScheme("hdfs");
    AbstractAvroSerDeTransformation transformation = new AvroSerDePartitionTransformation(config, null);
    assertEquals("hdfs:/etl/EAN/avsc/tlog_throttled_event.avsc", transformation.constructSource("/etl/EAN/avsc/tlog_throttled_event.avsc"));
  }

  @Test
  public void constructSourceOverridesScheme() {
    AvroSerDeConfig config = new AvroSerDeConfig();
    config.setSourceAvroUrlScheme("hdfs");
    AbstractAvroSerDeTransformation transformation = new AvroSerDePartitionTransformation(config, null);
    assertEquals("hdfs:/etl/EAN/avsc/tlog_throttled_event.avsc", transformation.constructSource("s3:///etl/EAN/avsc/tlog_throttled_event.avsc"));
  }

  @Test
  public void constructSourceWithoutScheme() {
    AvroSerDeConfig config = new AvroSerDeConfig();
    AbstractAvroSerDeTransformation transformation = new AvroSerDePartitionTransformation(config, null);
    assertEquals("/etl/EAN/avsc/tlog_throttled_event.avsc", transformation.constructSource("/etl/EAN/avsc/tlog_throttled_event.avsc"));
  }
}
