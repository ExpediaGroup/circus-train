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
package com.hotels.bdp.circustrain.metrics.conf;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import com.google.common.collect.ImmutableList;

public class GraphiteValidatorIntegrationTest {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private final Configuration conf = new Configuration();
  private GraphiteValidator validator;
  private File clusterProperties;

  @Before
  public void before() throws IOException {
    clusterProperties = new File(temp.getRoot(), "cluster.properties");
    Properties properties = new Properties();
    properties.put("graphite.host", "my.graphite.host:2003");
    properties.put("graphite.prefix", "chandler");
    try (OutputStream outputStream = new FileOutputStream(clusterProperties)) {
      properties.store(outputStream, null);
    }

    validator = new GraphiteValidator(conf);
  }

  @Test
  public void noArguments() {
    Graphite graphite = validate(ImmutableList.<String> of());

    assertThat(graphite.isEnabled(), is(false));
    assertThat(graphite.getConfig(), is(nullValue()));
    assertThat(graphite.getHost(), is(nullValue()));
    assertThat(graphite.getPrefix(), is(nullValue()));
    assertThat(graphite.getNamespace(), is(nullValue()));
  }

  @Test
  public void loadViaClusterProperties() throws IOException {
    List<String> args = ImmutableList
        .<String> builder()
        .add("--graphite.config=" + clusterProperties.getAbsolutePath())
        .add("--graphite.namespace=com.hotels.bdp.circus-train")
        .build();
    Graphite graphite = validate(args);

    assertThat(graphite.isEnabled(), is(true));
    assertThat(graphite.getConfig().toUri().toString(), is(clusterProperties.getAbsolutePath()));
    assertThat(graphite.getHost(), is("my.graphite.host:2003"));
    assertThat(graphite.getPrefix(), is("chandler"));
    assertThat(graphite.getNamespace(), is("com.hotels.bdp.circus-train"));
  }

  @Test
  public void loadViaClusterPropertiesEnvVar() throws IOException {
    System.setProperty("GRAPHITE_CONFIG", clusterProperties.getAbsolutePath());
    try {
      List<String> args = ImmutableList
          .<String> builder()
          .add("--graphite.namespace=com.hotels.bdp.circus-train")
          .build();
      Graphite graphite = validate(args);

      assertThat(graphite.isEnabled(), is(true));
      assertThat(graphite.getConfig().toUri().toString(), is(clusterProperties.getAbsolutePath()));
      assertThat(graphite.getHost(), is("my.graphite.host:2003"));
      assertThat(graphite.getPrefix(), is("chandler"));
      assertThat(graphite.getNamespace(), is("com.hotels.bdp.circus-train"));
    } finally {
      System.clearProperty("GRAPHITE_CONFIG");
    }
  }

  @Test
  public void loadViaConfig() throws IOException {
    List<String> args = ImmutableList
        .<String> builder()
        .add("--graphite.host=foo:1234")
        .add("--graphite.prefix=bar")
        .add("--graphite.namespace=com.hotels.bdp.circus-train")
        .build();
    Graphite graphite = validate(args);

    assertThat(graphite.isEnabled(), is(true));
    assertThat(graphite.getConfig(), is(nullValue()));
    assertThat(graphite.getHost(), is("foo:1234"));
    assertThat(graphite.getPrefix(), is("bar"));
    assertThat(graphite.getNamespace(), is("com.hotels.bdp.circus-train"));
  }

  @Test
  public void overrideClusterPropertiesInConfig() throws IOException {
    List<String> args = ImmutableList
        .<String> builder()
        .add("--graphite.config=" + clusterProperties.getAbsolutePath())
        .add("--graphite.host=foo:1234")
        .add("--graphite.namespace=com.hotels.bdp.circus-train")
        .build();
    Graphite graphite = validate(args);

    assertThat(graphite.isEnabled(), is(true));
    assertThat(graphite.getConfig().toUri().toString(), is(clusterProperties.getAbsolutePath()));
    assertThat(graphite.getHost(), is("foo:1234"));
    assertThat(graphite.getPrefix(), is("chandler"));
    assertThat(graphite.getNamespace(), is("com.hotels.bdp.circus-train"));
  }

  @org.springframework.context.annotation.Configuration
  @EnableConfigurationProperties(Graphite.class)
  static class TestConfig {}

  private Graphite validate(List<String> args) {
    Graphite graphite = SpringApplication
        .run(TestConfig.class, args.toArray(new String[args.size()]))
        .getBean(Graphite.class);
    return validator.validate(graphite).getGraphite();
  }

}
