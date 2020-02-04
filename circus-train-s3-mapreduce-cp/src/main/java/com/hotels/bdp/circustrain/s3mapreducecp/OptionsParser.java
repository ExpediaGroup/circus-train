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
package com.hotels.bdp.circustrain.s3mapreducecp;

import java.util.Arrays;

import org.apache.hadoop.fs.Path;

import com.beust.jcommander.JCommander;

/**
 * The OptionsParser parses out the command-line options passed to S3MapReduceCp and interprets those specific to
 * S3MapReduceCp to create an Options object.
 */
public class OptionsParser {

  private final S3MapReduceCpOptions options = new S3MapReduceCpOptions();
  private final JCommander jCommander = new JCommander(options);

  /**
   * The parse method parses the command-line options and creates a corresponding Options object.
   *
   * @param args Command-line arguments
   * @return The Options object, corresponding to the specified command-line.
   * @throws IllegalArgumentException Thrown if the parse fails.
   */
  public S3MapReduceCpOptions parse(String... args) throws IllegalArgumentException {
    try {
      jCommander.parse(args);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to parse arguments: " + Arrays.toString(args), e);
    }

    if (options.isHelp()) {
      return options;
    }

    for (Path source : options.getSources()) {
      if (!source.isAbsolute()) {
        throw new IllegalArgumentException("Source paths must be absolute: " + Arrays.toString(args));
      }
    }

    if (!options.getTarget().isAbsolute()) {
      throw new IllegalArgumentException("Destination URI must be absolute: " + Arrays.toString(args));
    }

    if (options.getCredentialsProvider() != null && !options.getCredentialsProvider().isAbsolute()) {
      throw new IllegalArgumentException("Credentials provider URI must be absolute: " + Arrays.toString(args));
    }

    if (options.getMaxMaps() <= 0) {
      options.setMaxMaps(1);
    }

    if (options.getLogPath() != null && !options.getLogPath().isAbsolute()) {
      throw new IllegalArgumentException("Log path must be absolute: " + Arrays.toString(args));
    }

    return options;
  }

  public void usage() {
    jCommander.usage();
  }

}
