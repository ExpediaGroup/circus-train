/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.util.TestDistCpUtils} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/util/TestDistCpUtils.java
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
package com.hotels.bdp.circustrain.s3mapreducecp.util;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.Closeable;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class IoUtilTest {

  private @Mock Logger logger;
  private @Mock Closeable closeable;

  @Test
  public void closeSilentlyNoCloseable() {
    IoUtil.closeSilently(logger);
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void closeSilentlyTypical() throws Exception {
    IoUtil.closeSilently(logger, closeable);
    verify(closeable).close();
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void closeSilentlyWhenCloseThrowsException() throws Exception {
    RuntimeException e = new RuntimeException();
    doThrow(e).when(closeable).close();
    IoUtil.closeSilently(logger, closeable);
    verify(closeable).close();
    verify(logger).debug(anyString(), eq(closeable), eq(e));
  }

}
