/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.util.TestRetriableCommand} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/util/TestRetriableCommand.java
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
package com.hotels.bdp.circustrain.s3mapreducecp.command;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.junit.Assert;
import org.junit.Test;

public class RetriableCommandTest {

  private static class MyRetriableCommand extends RetriableCommand<Object> {

    private int succeedAfter;
    private int retryCount = 0;

    public MyRetriableCommand(int succeedAfter) {
      super("MyRetriableCommand");
      this.succeedAfter = succeedAfter;
    }

    public MyRetriableCommand(int succeedAfter, RetryPolicy retryPolicy) {
      super("MyRetriableCommand", retryPolicy);
      this.succeedAfter = succeedAfter;
    }

    @Override
    protected Object doExecute(Object... arguments) throws Exception {
      if (++retryCount < succeedAfter) {
        throw new Exception("Transient failure#" + retryCount);
      }
      return 0;
    }
  }

  @Test
  public void typical() {
    try {
      new MyRetriableCommand(5).execute(0);
      Assert.assertTrue(false);
    } catch (Exception e) {
      Assert.assertTrue(true);
    }

    try {
      new MyRetriableCommand(3).execute(0);
      Assert.assertTrue(true);
    } catch (Exception e) {
      Assert.assertTrue(false);
    }

    try {
      new MyRetriableCommand(5, RetryPolicies.retryUpToMaximumCountWithFixedSleep(5, 0, TimeUnit.MILLISECONDS))
          .execute(0);
      Assert.assertTrue(true);
    } catch (Exception e) {
      Assert.assertTrue(false);
    }
  }

}
