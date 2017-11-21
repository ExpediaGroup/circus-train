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
package com.hotels.bdp.circustrain.housekeeping.service.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class EventIdExtractorTest {
  @Test
  public void extractEventIdCapturesCurrentEventIds() throws Exception {
    //eventId starts with ctp is still captured
    Path path = new Path(
        "s3://foo-baz-lab-abc-applications/hive/abc/123/ctp-20170918t145212.656z-3z0yaokj/region=ASIA");
    assertEquals("ctp-20170918t145212.656z-3z0yaokj", EventIdExtractor.extractFrom(path));

    //eventId starts with ctt is still captured
    path = new Path(
        "s3://foo-baz-lab-abc-applications/hive/abc/123/ctt-20170918t145212.656z-3z0yaokj/region=ASIA");
    assertEquals("ctt-20170918t145212.656z-3z0yaokj", EventIdExtractor.extractFrom(path));

    //Case insensitive
    path = new Path(
        "S3://FOO-BAZ-LAB-ABC-APPLICATIONS/HIVE/ABC/123/CTT-20170918T145212.656Z-3Z0YAOKJ/REGION=ASIA");
    assertEquals("ctt-20170918t145212.656z-3z0yaokj", EventIdExtractor.extractFrom(path).toLowerCase());
  }

  @Test
  public void extractEventIdCapturesLegacyEventIds() throws Exception {
    //eventId starts with ctp is still captured
    Path path = new Path(
        "hdfs://hadoop-foo-baz-lab-applications/hive/data/926/ctp-20170918t145212.656z-3z0yaokj/region=ASIA");
    assertEquals("ctp-20170918t145212.656z-3z0yaokj", EventIdExtractor.extractFrom(path));

    //eventId starts with ctt is still captured
    path = new Path(
        "hdfs://hadoop-foo-baz-lab-applications/hive/data/926/ctt-20170918t145212.656z-3z0yaokj/region=ASIA");
    assertEquals("ctt-20170918t145212.656z-3z0yaokj", EventIdExtractor.extractFrom(path));
  }

  @Test
  public void extractEventIdReturnsNullWhenCorrectEventIdDoesntExist() throws Exception {
    //No eventId
    Path path = new Path(
        "s3://foo-baz-lab-abc-applications/hive/abc/123/region=ASIA");
    assertNull(EventIdExtractor.extractFrom(path));

    //Empty path
    path = new Path("gs://");
    assertNull(EventIdExtractor.extractFrom(path));

    //Not full eventId
    path = new Path("s3://foo-baz-lab-abc-applications/hive/abc/123/ctp-2017/region=ASIA");
    assertNull(EventIdExtractor.extractFrom(path));

    //eventId short a character
    path = new Path("s3://foo-baz-lab-abc-applications/hive/abc/123/ctt-20170918t145212.656z-3z0yaok/region=ASIA");
    assertNull(EventIdExtractor.extractFrom(path));

    //eventId has one character too many
    path = new Path("s3://foo-baz-lab-abc-applications/hive/abc/123/ctt-20170918t145212.656z-3z0yaokja/region=ASIA");
    assertNull(EventIdExtractor.extractFrom(path));
  }

  @Test(expected = NullPointerException.class)
  public void nullPathThrowsException() {
    EventIdExtractor.extractFrom(null);
  }
}