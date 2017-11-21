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
package com.hotels.bdp.circustrain.hive.iterator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.thrift.TException;
import org.junit.Test;

public class BatchResolverTest {

  private final List<String> partitionNames = Arrays.asList("a=02", "a=03", "a=01", "a=05", "a=04");

  @Test
  public void batchSizeOfOne() throws TException {
    BatchResolver batchBoundaryResolver = new BatchResolver(partitionNames, (short) 1);
    assertThat(batchBoundaryResolver.resolve(), is(Arrays.asList(Arrays.asList("a=02"), Arrays.asList("a=03"),
        Arrays.asList("a=01"), Arrays.asList("a=05"), Arrays.asList("a=04"))));
  }

  @Test
  public void multiBatch() throws TException {
    BatchResolver batchBoundaryResolver = new BatchResolver(partitionNames, (short) 2);
    assertThat(batchBoundaryResolver.resolve(),
        is(Arrays.asList(Arrays.asList("a=02", "a=03"), Arrays.asList("a=01", "a=05"), Arrays.asList("a=04"))));
  }

  @Test
  public void overflowsIntoSecondBatch() throws TException {
    BatchResolver batchBoundaryResolver = new BatchResolver(partitionNames, (short) 4);
    assertThat(batchBoundaryResolver.resolve(),
        is(Arrays.asList(Arrays.asList("a=02", "a=03", "a=01", "a=05"), Arrays.asList("a=04"))));
  }

  @Test
  public void fitsInOneBatch() throws TException {
    BatchResolver batchBoundaryResolver = new BatchResolver(partitionNames, (short) 5);
    assertThat(batchBoundaryResolver.resolve(),
        is(Arrays.asList(Arrays.asList("a=02", "a=03", "a=01", "a=05", "a=04"))));
  }

  @Test
  public void noPartitions() throws TException {
    BatchResolver batchBoundaryResolver = new BatchResolver(Collections.<String> emptyList(), (short) 5);
    assertThat(batchBoundaryResolver.resolve(), is(Arrays.asList(Collections.<String> emptyList())));
  }

}
