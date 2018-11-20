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
package com.hotels.bdp.circustrain.housekeeping;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.housekeeping.listener.JdbcHousekeepingListener;
import com.hotels.housekeeping.model.LegacyReplicaPath;
import com.hotels.housekeeping.service.HousekeepingService;

@RunWith(MockitoJUnitRunner.class)
public class JdbcHousekeepingListenerTest {

  private static final String EVENT_ID = "eventId";
  private static final String PATH_EVENT_ID = "pathEventId";

  private @Mock HousekeepingService cleanUpPathService;

  @InjectMocks
  private final JdbcHousekeepingListener jdbcCleanUpLocationListener = new JdbcHousekeepingListener();

  @Test
  public void typical() {
    ArgumentCaptor<LegacyReplicaPath> argumentCaptor = ArgumentCaptor.forClass(LegacyReplicaPath.class);
    doNothing().when(cleanUpPathService).scheduleForHousekeeping(argumentCaptor.capture());
    Path location = new Path("/foo/bar");
    jdbcCleanUpLocationListener.cleanUpLocation(EVENT_ID, PATH_EVENT_ID, location, "db", "table");
    verify(cleanUpPathService).scheduleForHousekeeping(any(LegacyReplicaPath.class));
    assertThat(argumentCaptor.getValue().getEventId(), is(EVENT_ID));
    assertThat(argumentCaptor.getValue().getPathEventId(), is(PATH_EVENT_ID));
    assertThat(argumentCaptor.getValue().getPath(), is("/foo/bar"));
    assertThat(argumentCaptor.getValue().getMetastoreDatabaseName(), is("db"));
    assertThat(argumentCaptor.getValue().getMetastoreTableName(), is("table"));
  }
}
