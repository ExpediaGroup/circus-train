/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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
package com.hotels.bdp.circustrain.avro.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static com.hotels.bdp.circustrain.avro.util.AvroStringUtils.appendForwardSlashIfNotPresent;
import static com.hotels.bdp.circustrain.avro.util.AvroStringUtils.argsPresent;
import static com.hotels.bdp.circustrain.avro.util.AvroStringUtils.avroDestination;

import org.junit.Test;

public class AvroStringUtilsTest {

  @Test
  public void avroDestinationTest() {
    assertThat(avroDestination("file://dummy/url/", "123/", "location"), is("file://dummy/url/123/"));
    assertThat(avroDestination("file://dummy/url", "123", "location"), is("file://dummy/url/123/"));
  }

  @Test
  public void avroDestinationIsSameAsReplicaLocationTest() {
    assertThat(avroDestination("file://dummy/url/", "123/", "file://dummy/url/"), is("file://dummy/url/123/.schema"));
    assertThat(avroDestination("file://dummy/url", "123", "file://dummy/url/"), is("file://dummy/url/123/.schema"));
  }

  @Test
  public void avroDestinationIsHiddenFolder() {
    String[] folders = avroDestination("dummy/url/", "123", "dummy/url").split("/");
    assertThat(folders[folders.length - 1], startsWith("."));
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullDestinationFolderParamTest() {
    avroDestination(null, "123", "location");
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyDestinationFolderParamTest() {
    avroDestination("", "123", "location");
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullEventIdParamTest() {
    avroDestination("test", null, "location");
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyEventIdParamTest() {
    avroDestination("", null, "location");
  }

  @Test
  public void appendForwardSlashWhenNoneIsPresent() {
    assertThat(appendForwardSlashIfNotPresent("test"), is("test/"));
  }

  @Test
  public void doesntAppendSecondForwardSlashWhenOneIsPresent() {
    assertThat(appendForwardSlashIfNotPresent("test/"), is("test/"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullArgParamTest() {
    appendForwardSlashIfNotPresent(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyArgParamTest() {
    appendForwardSlashIfNotPresent("");
  }

  @Test
  public void argsPresentTest() {
    assertTrue(argsPresent("test", "strings", "are", "present"));
  }

  @Test
  public void argsPresentShouldFailWithNullStringTest() {
    assertFalse(argsPresent("test", "strings", null, "present"));
  }

  @Test
  public void argsPresentShouldFailWithEmptyStringTest() {
    assertFalse(argsPresent("test", "strings", "", "present"));
  }
}
