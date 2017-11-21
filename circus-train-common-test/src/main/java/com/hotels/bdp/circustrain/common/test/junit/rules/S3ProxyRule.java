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
package com.hotels.bdp.circustrain.common.test.junit.rules;

import java.net.URI;
import java.util.Properties;

import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.gaul.s3proxy.AuthenticationType;
import org.gaul.s3proxy.S3Proxy;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emulates a S3 endpoint.
 */
public class S3ProxyRule extends ExternalResource {
  private static final Logger LOG = LoggerFactory.getLogger(S3ProxyRule.class);

  private static final String ENDPOINT_URL = "http://127.0.0.1";

  public static class Builder {
    private S3Proxy.Builder s3ProxyBuilder = S3Proxy.builder();
    private String accessKey;
    private String secretKey;
    private int port = -1;
    private boolean ignoreUnknownHeaders;

    private Builder() {}

    public Builder withCredentials(String accessKey, String secretKey) {
      this.accessKey = accessKey;
      this.secretKey = secretKey;
      s3ProxyBuilder.awsAuthentication(AuthenticationType.AWS_V2_OR_V4, accessKey, secretKey);
      return this;
    }

    public Builder withSecretStore(String path, String password) {
      s3ProxyBuilder.keyStore(path, password);
      return this;
    }

    public Builder withPort(int port) {
      this.port = port;
      return this;
    }

    public Builder ignoreUnknownHeaders() {
      ignoreUnknownHeaders = true;
      return this;
    }

    public S3ProxyRule build() {
      return new S3ProxyRule(this);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private TemporaryFolder temp = new TemporaryFolder();
  private S3Proxy.Builder s3ProxyBuilder;
  private final boolean ignoreUnknownHeaders;
  private final String accessKey;
  private final String secretKey;
  private S3Proxy s3Proxy;
  private int port;

  private S3ProxyRule(Builder builder) {
    s3ProxyBuilder = builder.s3ProxyBuilder;
    ignoreUnknownHeaders = builder.ignoreUnknownHeaders;
    accessKey = builder.accessKey;
    secretKey = builder.secretKey;
    port = builder.port < 0 ? 0 : builder.port;
  }

  @Override
  protected void before() throws Throwable {
    temp.create();

    Properties properties = new Properties();
    properties.setProperty("jclouds.filesystem.basedir", temp.newFolder("blobStore").getCanonicalPath());

    BlobStoreContext context = ContextBuilder
        .newBuilder("filesystem")
        .credentials("access", "secret")
        .overrides(properties)
        .build(BlobStoreContext.class);

    s3Proxy = s3ProxyBuilder
        .blobStore(context.getBlobStore())
        .endpoint(URI.create(String.format("%s:%d", ENDPOINT_URL, port)))
        .ignoreUnknownHeaders(ignoreUnknownHeaders)
        .build();

    LOG.debug("S3 proxy is starting");
    s3Proxy.start();
    while (!s3Proxy.getState().equals(AbstractLifeCycle.STARTED)) {
      Thread.sleep(10);
    }
    LOG.debug("S3 proxy is running");
    port = s3Proxy.getPort();

    s3ProxyBuilder = null;
  }

  @Override
  protected void after() {
    LOG.debug("S3 proxy is stopping");
    try {
      s3Proxy.stop();
    } catch (Exception e) {
      throw new RuntimeException("Unable to stop S3 proxy", e);
    } finally {
      temp.delete();
    }
    LOG.debug("S3 proxy has stopped");
  }

  public String getProxyUrl() {
    return String.format("%s:%d", ENDPOINT_URL, s3Proxy.getPort());
  }

  public String getAccessKey() {
    return accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

}
