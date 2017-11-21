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
package com.hotels.bdp.circustrain.tool.comparison;

import java.io.File;
import java.util.List;

import org.springframework.boot.ApplicationArguments;

public class ComparisonToolArgs {

  public final static String OUTPUT_FILE = "outputFile";
  public final static String SOURCE_PARTITION_BATCH_SIZE = "sourcePartitionBatchSize";
  public final static String REPLICA_PARTITION_BUFFER_SIZE = "replicaPartitionBufferSize";
  private File outputFile;

  public ComparisonToolArgs(ApplicationArguments args) {
    validate(args);
  }

  private void validate(ApplicationArguments args) {
    List<String> optionValues = args.getOptionValues(OUTPUT_FILE);
    if (optionValues == null) {
      throw new IllegalArgumentException("Missing --" + OUTPUT_FILE + " argument");
    } else if (optionValues.isEmpty()) {
      throw new IllegalArgumentException("Missing --" + OUTPUT_FILE + " argument value");
    }
    outputFile = new File(optionValues.get(0));
  }

  public File getOutputFile() {
    return outputFile;
  }

}
