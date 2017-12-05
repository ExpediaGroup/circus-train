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
package com.hotels.bdp.circustrain.housekeeping.conf;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import org.joda.time.Period;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hotels.bdp.circustrain.converter.StringToDurationConverter;

@Configuration
@ComponentScan
@EnableAutoConfiguration
@Profile("HousekeepingTest")
class TestConfiguration {
  @Bean
  @ConfigurationPropertiesBinding
  StringToDurationConverter stringToDurationConverter() {
    return new StringToDurationConverter();
  }
}

@RunWith(SpringJUnit4ClassRunner.class)
@TestPropertySource(properties = {
    "project.artifact-id=my-artifact",
    "project.name=myproject",
    "housekeeping.expired-path-duration=P7D",
    "housekeeping.data-source.driver-class-name=org.h2.Driver",
    "housekeeping.data-source.url=jdbc:h2:/opt/${project.artifact-id}/data/${project.name}/housekeeping;AUTO_SERVER=TRUE;DB_CLOSE_ON_EXIT=FALSE",
    "housekeeping.data-source.username=user",
    "housekeeping.data-source.password=password" })
@ContextConfiguration(classes = {
    TestConfiguration.class }, initializers = ConfigFileApplicationContextInitializer.class)
@ActiveProfiles("HousekeepingTest")
public class HousekeepingIntegrationTest {

  private @Autowired Housekeeping housekeeping;

  @Test
  public void typical() {
    assertThat(housekeeping.getExpiredPathDuration(), is(not(nullValue())));
    assertThat(housekeeping.getExpiredPathDuration(), is(new Period(168, 0, 0, 0).toStandardDuration()));
    assertThat(housekeeping.getDataSource(), is(not(nullValue())));
    assertThat(housekeeping.getDataSource().getDriverClassName(), is("org.h2.Driver"));
    assertThat(housekeeping.getDataSource().getUrl(),
        is("jdbc:h2:/opt/my-artifact/data/myproject/housekeeping;AUTO_SERVER=TRUE;DB_CLOSE_ON_EXIT=FALSE"));
    assertThat(housekeeping.getDataSource().getUsername(), is("user"));
    assertThat(housekeeping.getDataSource().getPassword(), is("password"));
  }
}
