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
package com.hotels.bdp.circustrain.distcpcopier;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.TreeTraverser;

import com.hotels.bdp.circustrain.api.CircusTrainException;

class FileStatusTreeTraverser extends TreeTraverser<FileStatus> {

  private final FileSystem fileSystem;

  FileStatusTreeTraverser(FileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  @Override
  public Iterable<FileStatus> children(FileStatus root) {
    if (root.isFile()) {
      return ImmutableList.of();
    }
    try {
      FileStatus[] listStatus = fileSystem.listStatus(root.getPath());
      if (listStatus == null || listStatus.length == 0) {
        return ImmutableList.of();
      }
      return ImmutableList.copyOf(listStatus);
    } catch (IOException e) {
      throw new CircusTrainException("Unable to list children for path: " + root.getPath());
    }
  }

}
