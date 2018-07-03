/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
package com.hotels.bdp.circustrain.core.conf;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

import com.hotels.bdp.circustrain.conf.ReplicaCatalog;

public class ReplicaCatalogIntegrationTest {

  public @Rule TemporaryFolder temp = new TemporaryFolder();

  private File ymlFile;

  @Before
  public void before() {
    ymlFile = new File(temp.getRoot(), "application.yml");
  }

  @Test
  public void test() throws IOException {
    writeYmlFile();
    String[] args = new String[] { "--spring.config.location=" + ymlFile.getAbsolutePath() };
    ConfigurableApplicationContext context = SpringApplication.run(TestConfig.class, args);
    ReplicaCatalog replicaCatalog = context.getBean(ReplicaCatalog.class);
    Map<String, String> map = replicaCatalog.getConfigurationProperties();

    assertThat(map.size(), is(1));
    assertThat(map.get("a"), is("b"));
  }

  @Configuration
  @EnableConfigurationProperties(CircusTrainReplicaCatalog.class)
  static class TestConfig {}

  private void writeYmlFile() throws IOException {
    List<String> lines = ImmutableList
        .<String> builder()
        .add("replica-catalog:")
        .add("  name: blah")
        .add("  hive-metastore-uris: thrift://foo:1234")
        .add("  configuration-properties:")
        .add("    a: b")
        .build();
    Files.asCharSink(ymlFile, UTF_8).writeLines(lines);
  }

}
