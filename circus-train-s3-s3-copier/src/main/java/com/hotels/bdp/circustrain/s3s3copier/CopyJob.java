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