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
package com.hotels.bdp.circustrain.housekeeping.repository;

import static org.hamcrest.CoreMatchers.is;
import static org.joda.time.Instant.parse;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.util.List;

import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.config.DelegatingApplicationContextInitializer;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.transaction.annotation.Transactional;

import com.github.springtestdbunit.DbUnitTestExecutionListener;
import com.github.springtestdbunit.TransactionDbUnitTestExecutionListener;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DbUnitConfiguration;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import com.hotels.bdp.circustrain.TestApplication;
import com.hotels.bdp.circustrain.housekeeping.model.LegacyReplicaPath;

@RunWith(SpringJUnit4ClassRunner.class)
@TestPropertySource(properties = {
    "spring.jpa.database=H2",
    "spring.jpa.hibernate.ddl-auto=update",
    "spring.jpa.hibernate.generate-ddl=true",
    "spring.datasource.initialize=true",
    "spring.datasource.schema=classpath:/schema.sql",
    "spring.datasource.max-wait=10000",
    "spring.datasource.max-active=5",
    "spring.datasource.test-on-borrow=true",
    "housekeeping.expired-path-duration=P3D",
    "housekeeping.data-source.driver-class-name=org.h2.Driver",
    "housekeeping.data-source.url=jdbc:h2:./target/data/housekeeping;AUTO_SERVER=TRUE;DB_CLOSE_ON_EXIT=FALSE",
    "housekeeping.data-source.username=bdp",
    "housekeeping.data-source.password=Ch4ll3ng3" })
@SpringApplicationConfiguration(classes = { TestApplication.class }, initializers = {
    DelegatingApplicationContextInitializer.class })
@TestExecutionListeners({
    DependencyInjectionTestExecutionListener.class,
    DirtiesContextTestExecutionListener.class,
    TransactionDbUnitTestExecutionListener.class,
    DbUnitTestExecutionListener.class })
@DbUnitConfiguration(databaseConnection = "dbUnitDatabaseConnection")
public class LegacyReplicaPathRepositoryTest {

  @Autowired
  private LegacyReplicaPathRepository repository;

  @AfterClass
  public static void teardown() {
    File db = new File("./target/data/");
    db.deleteOnExit();
  }

  @Test
  @Transactional
  @Rollback
  @DatabaseSetup("classpath:/com/hotels/bdp/circustrain/housekeeping/repository/legacyReplicaPath-initial-test-data.xml")
  public void findByCreationTimestampLessThanEqual() {
    Instant referenceData = parse("2016-04-07T11:25:15Z");
    List<LegacyReplicaPath> searchResults = repository.findByCreationTimestampLessThanEqual(referenceData.getMillis());
    assertThat(searchResults.size(), is(2));
    assertCleanUpPath(searchResults.get(0), 1L, "eventId_1", "eventId_0", "file:/foo/bar/0",
        parse("2016-04-07T11:13:28Z"));
    assertCleanUpPath(searchResults.get(1), 2L, "eventId_2", "eventId_1", "file:/foo/bar/1",
        parse("2016-04-07T11:25:15Z"));
  }

  @Test
  @DatabaseSetup("classpath:/com/hotels/bdp/circustrain/housekeeping/repository/legacyReplicaPath-initial-test-data.xml")
  @ExpectedDatabase(value = "classpath:/com/hotels/bdp/circustrain/housekeeping/repository/legacyReplicaPath-after-save-test.xml", assertionMode = DatabaseAssertionMode.NON_STRICT)
  public void save() {
    repository.save(new LegacyReplicaPath("eventId_4", "eventId_3", "file:/foo/bar/3"));
  }

  private void assertCleanUpPath(
      LegacyReplicaPath actual,
      long expectedId,
      String expectedEventId,
      String expectedPathEventId,
      String expectedPath,
      Instant expectedCreationTimestamp) {
    assertThat(actual.getId(), is(expectedId));
    assertThat(actual.getEventId(), is(expectedEventId));
    assertThat(actual.getPathEventId(), is(expectedPathEventId));
    assertThat(actual.getPath(), is(expectedPath));
    assertThat(actual.getCreationTimestamp(), is(expectedCreationTimestamp.getMillis()));
  }
}
