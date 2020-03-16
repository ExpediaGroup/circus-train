package com.hotels.bdp.circustrain.s3s3copier.aws;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import java.util.HashMap;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;

import com.hotels.bdp.circustrain.s3s3copier.S3S3CopierOptions;

@RunWith(MockitoJUnitRunner.class)
public class TransferManagerFactoryTest {

  @Mock private AmazonS3 mockClient;
  private final Long MULTIPART_COPY_THRESHOLD_VALUE = 1L;
  private final Long MULTIPART_COPY_PART_SIZE = 1L;

  @Test
  public void shouldCreateDefaultTransferManagerClient() {
    S3S3CopierOptions s3Options = new S3S3CopierOptions(new HashMap<String, Object>() {{
      put(S3S3CopierOptions.Keys.MULTIPART_COPY_THRESHOLD.keyName(), MULTIPART_COPY_THRESHOLD_VALUE);
      put(S3S3CopierOptions.Keys.MULTIPART_COPY_PART_SIZE.keyName(), MULTIPART_COPY_PART_SIZE);
    }});

    TransferManagerFactory factory = new TransferManagerFactory();
    TransferManager transferManager = factory.newInstance(mockClient, s3Options);
    assertThat(transferManager.getAmazonS3Client(), is(mockClient));

    TransferManagerConfiguration managerConfig = transferManager.getConfiguration();
    assertThat(managerConfig.getMultipartCopyPartSize(), is(MULTIPART_COPY_PART_SIZE));
    assertThat(managerConfig.getMultipartCopyThreshold(), is(MULTIPART_COPY_THRESHOLD_VALUE));
  }
}
