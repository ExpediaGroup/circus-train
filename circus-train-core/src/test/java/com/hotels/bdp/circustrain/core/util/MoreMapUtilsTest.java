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
package com.hotels.bdp.circustrain.core.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import static com.hotels.bdp.circustrain.core.util.MoreMapUtilsTest.MyEnum.ONE;
import static com.hotels.bdp.circustrain.core.util.MoreMapUtilsTest.MyEnum.TWO_AND_A_HALF;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.google.common.collect.Lists;

public class MoreMapUtilsTest {

  private static final String KEY = "key";

  public static enum MyEnum {
    ONE,
    TWO_AND_A_HALF;
  }

  private Map<?, ?> newMap(Object key, Object value) {
    Map<Object, Object> map = new HashMap<>();
    map.put(key, value);
    return map;
  }

  @Test
  public void listOfEnumSingleStringValue() {
    Map<?, ?> map = newMap(KEY, ONE.name());
    List<MyEnum> actualEnums = MoreMapUtils.getListOfEnum(map, KEY, null, MyEnum.class);
    List<MyEnum> expectedEnums = Lists.newArrayList(ONE);
    assertThat(actualEnums, is(expectedEnums));
  }

  @Test
  public void listOfEnumSingleValue() {
    Map<?, ?> map = newMap(KEY, ONE);
    List<MyEnum> actualEnums = MoreMapUtils.getListOfEnum(map, KEY, null, MyEnum.class);
    List<MyEnum> expectedEnums = Lists.newArrayList(ONE);
    assertThat(actualEnums, is(expectedEnums));
  }

  @Test
  public void listOfEnumMultiStringValue() {
    Map<?, ?> map = newMap(KEY, Lists.newArrayList(ONE.name(), TWO_AND_A_HALF.name()));
    List<MyEnum> actualEnums = MoreMapUtils.getListOfEnum(map, KEY, null, MyEnum.class);
    List<MyEnum> expectedEnums = Lists.newArrayList(ONE, TWO_AND_A_HALF);
    assertThat(actualEnums, is(expectedEnums));
  }

  @Test
  public void listOfEnumStringMultiValue() {
    Map<?, ?> map = newMap(KEY, ONE.name() + ", " + TWO_AND_A_HALF.name());
    List<MyEnum> actualEnums = MoreMapUtils.getListOfEnum(map, KEY, null, MyEnum.class);
    List<MyEnum> expectedEnums = Lists.newArrayList(ONE, TWO_AND_A_HALF);
    assertThat(actualEnums, is(expectedEnums));
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidHadoopPath() {
    Map<?, ?> map = newMap(KEY, Integer.valueOf(100));
    MoreMapUtils.getHadoopPath(map, KEY, null);
  }

  @Test
  public void noKeyReturnsDefaultHadoopPath() {
    Map<?, ?> map = newMap(KEY + "_+", "abc");
    Path defaultPath = new Path("myPath");
    assertThat(MoreMapUtils.getHadoopPath(map, KEY, defaultPath), is(defaultPath));
  }

  @Test
  public void nullHadoopPath() {
    Map<?, ?> map = newMap(KEY, null);
    assertThat(MoreMapUtils.getHadoopPath(map, KEY, null), is(nullValue()));
  }

  @Test
  public void hadoopPath() {
    Path path = new Path("myPath");
    Map<?, ?> map = newMap(KEY, path);
    assertThat(MoreMapUtils.getHadoopPath(map, KEY, null), is(path));
  }

  @Test
  public void hadoopPathAsString() {
    Map<?, ?> map = newMap(KEY, "myStringPath");
    assertThat(MoreMapUtils.getHadoopPath(map, KEY, null), is(new Path("myStringPath")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidPathType() {
    Map<?, ?> map = newMap(KEY, new Object());
    MoreMapUtils.getHadoopPath(map, KEY, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidUri() {
    Map<?, ?> map = newMap(KEY, "s3:");
    MoreMapUtils.getUri(map, KEY, null);
  }

  @Test
  public void noKeyReturnsDefaultUri() {
    Map<?, ?> map = newMap(KEY + "_+", "abc");
    URI defaultUri = URI.create("my-uri");
    assertThat(MoreMapUtils.getUri(map, KEY, defaultUri), is(defaultUri));
  }

  @Test
  public void nullUri() {
    Map<?, ?> map = newMap(KEY, null);
    assertThat(MoreMapUtils.getUri(map, KEY, null), is(nullValue()));
  }

  @Test
  public void uri() {
    URI uri = URI.create("http://localhost:8080/");
    Map<?, ?> map = newMap(KEY, uri);
    assertThat(MoreMapUtils.getUri(map, KEY, null), is(uri));
  }

  @Test
  public void uriAsString() {
    Map<?, ?> map = newMap(KEY, "ftp://localhost:8080/foo/bar/");
    assertThat(MoreMapUtils.getUri(map, KEY, null), is(URI.create("ftp://localhost:8080/foo/bar/")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidUriType() {
    Map<?, ?> map = newMap(KEY, new Object());
    MoreMapUtils.getUri(map, KEY, null);
  }

}
