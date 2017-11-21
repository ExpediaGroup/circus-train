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

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.tools.util.DistCpUtils;

import com.google.common.base.Function;

import com.hotels.bdp.circustrain.api.CircusTrainException;

class CopyListingFileStatusFunction implements Function<FileStatus, CopyListingFileStatus> {

  private final FileSystem fileSystem;
  private final boolean preserveXAttrs;
  private final boolean preserveAcls;
  private final boolean preserveRawXAttrs;

  CopyListingFileStatusFunction(FileSystem fileSystem, DistCpOptions options) {
    this.fileSystem = fileSystem;
    preserveAcls = options.shouldPreserve(FileAttribute.ACL);
    preserveXAttrs = options.shouldPreserve(FileAttribute.XATTR);
    preserveRawXAttrs = options.shouldPreserveRawXattrs();
  }

  @Override
  public CopyListingFileStatus apply(FileStatus fileStatus) {
    try {
      return DistCpUtils.toCopyListingFileStatus(fileSystem, fileStatus, preserveAcls, preserveXAttrs,
          preserveRawXAttrs);
    } catch (IOException e) {
      throw new CircusTrainException("Error transforming to CopyListingFileStatus: " + fileStatus, e);
    }
  }

}
