/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
package com.hotels.bdp.circustrain.s3s3copier.aws;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.amazonaws.client.builder.ExecutorFactory;

public class ExecutorServiceFactory implements ExecutorFactory {
  private int threadServicePoolCount;

  public ExecutorServiceFactory(int threadPoolCount) {
    this.threadServicePoolCount = threadPoolCount;
  }

  @Override
  public ExecutorService newExecutor() {
    ThreadFactory threadFactory = new ThreadFactory() {
      private int threadCount = 1;
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName("s3-transfer-manager-worker-" + threadCount++);
        return thread;
      }
    };
    return Executors.newFixedThreadPool(threadServicePoolCount, threadFactory);
  }
}
