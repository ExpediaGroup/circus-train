/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.bdp.circustrain.s3s3copier;

import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.internal.TransferStateChangeListener;

public class CopyJob {
  private Copy copy;
  private CopyObjectRequest copyObjectRequest;
  private TransferStateChangeListener transferStateChangeListener;

  public CopyJob(Copy copy,
                 CopyObjectRequest copyObjectRequest,
                 TransferStateChangeListener transferStateChangeListener) {
    this.copy = copy;
    this.copyObjectRequest = copyObjectRequest;
    this.transferStateChangeListener = transferStateChangeListener;
  }

  public CopyJob(CopyObjectRequest copyObjectRequest,
                 TransferStateChangeListener transferStateChangeListener) {
    this(null, copyObjectRequest, transferStateChangeListener);
  }

  public Copy getCopy() {
    return copy;
  }

  public CopyObjectRequest getCopyObjectRequest() {
    return copyObjectRequest;
  }

  public TransferStateChangeListener getTransferStateChangeListener() {
    return transferStateChangeListener;
  }
}