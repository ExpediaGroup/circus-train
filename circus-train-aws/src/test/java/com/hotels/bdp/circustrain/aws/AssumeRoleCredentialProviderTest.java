package com.hotels.bdp.circustrain.aws;

import com.amazonaws.auth.AWSCredentials;
import org.apache.commons.lang.NullArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class AssumeRoleCredentialProviderTest {

    @Test(expected = NullPointerException.class)
    public void throwsNullPointerException() {
        AssumeRoleCredentialProvider provider = new AssumeRoleCredentialProvider(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsIllegalArgumentException() {
        AssumeRoleCredentialProvider provider = new AssumeRoleCredentialProvider(new Configuration());
    }
}
