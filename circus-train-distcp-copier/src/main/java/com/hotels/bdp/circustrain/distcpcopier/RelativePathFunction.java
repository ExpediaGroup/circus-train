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
package com.hotels.bdp.circustrain.distcpcopier;

import javax.annotation.Nonnull;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.util.DistCpUtils;

import com.google.common.base.Function;

class RelativePathFunction implements Function<FileStatus, String> {

  private final Path sourceRootPath;

  RelativePathFunction(Path sourceRootPath) {
    this.sourceRootPath = sourceRootPath;
  }

  @Override
  public String apply(@Nonnull FileStatus fileStatus) {
    return DistCpUtils.getRelativePath(sourceRootPath, fileStatus.getPath());
  }

}
