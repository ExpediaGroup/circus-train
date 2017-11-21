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
package com.hotels.bdp.circustrain.housekeeping.service.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.orm.ObjectOptimisticLockingFailureException;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.housekeeping.model.LegacyReplicaPath;
import com.hotels.bdp.circustrain.housekeeping.repository.LegacyReplicaPathRepository;

@RunWith(PowerMockRunner.class)
@PrepareForTest(FileSystem.class)
public class FileSystemHousekeepingServiceTest {

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final String EVENT_ID = "eventId-a";
  private static final String PATH_EVENT_ID = "pathEventId-n";

  private final Instant now = new Instant();
  private Path eventPath;
  private Path test1Path;
  private Path val1Path;
  private Path val2Path;
  private Path val3Path;
  private LegacyReplicaPath cleanUpPath1;
  private LegacyReplicaPath cleanUpPath2;
  private LegacyReplicaPath cleanUpPath3;

  private @Mock LegacyReplicaPathRepository legacyReplicationPathRepository;
  private @Spy final FileSystem spyFs = new LocalFileSystem();
  private final Configuration conf = new Configuration();

  private FileSystemHousekeepingService service;

  @Before
  public void init() throws Exception {
    spyFs.initialize(spyFs.getUri(), conf);
    mockStatic(FileSystem.class);
    when(FileSystem.get(any(URI.class), any(Configuration.class))).thenReturn(spyFs);
    eventPath = new Path(tmpFolder.newFolder("foo", "bar", PATH_EVENT_ID).getCanonicalPath());
    test1Path = new Path(tmpFolder.newFolder("foo", "bar", PATH_EVENT_ID, "test=1").getCanonicalPath());
    val1Path = new Path(tmpFolder.newFolder("foo", "bar", PATH_EVENT_ID, "test=1", "val=1").getCanonicalPath());
    val2Path = new Path(tmpFolder.newFolder("foo", "bar", PATH_EVENT_ID, "test=1", "val=2").getCanonicalPath());
    val3Path = new Path(tmpFolder.newFolder("foo", "bar", PATH_EVENT_ID, "test=1", "val=3").getCanonicalPath());
    service = new FileSystemHousekeepingService(legacyReplicationPathRepository, conf);
    cleanUpPath1 = new LegacyReplicaPath(EVENT_ID, PATH_EVENT_ID, val1Path.toString());
    cleanUpPath2 = new LegacyReplicaPath(EVENT_ID, PATH_EVENT_ID, val2Path.toString());
    cleanUpPath3 = new LegacyReplicaPath(EVENT_ID, PATH_EVENT_ID, val3Path.toString());
  }

  @Test
  public void sheduleForHousekeeping() {
    service.scheduleForHousekeeping(cleanUpPath1);
    verify(legacyReplicationPathRepository).save(cleanUpPath1);
  }

  @Test(expected = CircusTrainException.class)
  public void scheduleFails() {
    when(legacyReplicationPathRepository.save(cleanUpPath1)).thenThrow(new RuntimeException());
    service.scheduleForHousekeeping(cleanUpPath1);
  }

  @Test
  public void cleanUp() throws Exception {
    when(legacyReplicationPathRepository.findByCreationTimestampLessThanEqual(now.getMillis()))
        .thenReturn(Arrays.asList(cleanUpPath1, cleanUpPath2, cleanUpPath3));

    service.cleanUp(now);

    verify(legacyReplicationPathRepository).delete(cleanUpPath1);
    verify(legacyReplicationPathRepository).delete(cleanUpPath2);
    verify(legacyReplicationPathRepository).delete(cleanUpPath3);
    deleted(eventPath);
  }

  @Test
  public void eventuallyConsistentCleanUpFull() throws Exception {
    when(legacyReplicationPathRepository.findByCreationTimestampLessThanEqual(now.getMillis()))
        .thenReturn(Arrays.asList(cleanUpPath1, cleanUpPath2, cleanUpPath3));

    doReturn(false).when(spyFs).delete(val1Path, true);
    service.cleanUp(now);

    exists(val1Path);
    deleted(val2Path, val3Path);
    verify(legacyReplicationPathRepository, never()).delete(cleanUpPath1);
    verify(legacyReplicationPathRepository).delete(cleanUpPath2);
    verify(legacyReplicationPathRepository).delete(cleanUpPath3);

    reset(legacyReplicationPathRepository);
    when(legacyReplicationPathRepository.findByCreationTimestampLessThanEqual(now.getMillis()))
        .thenReturn(Arrays.asList(cleanUpPath1));
    reset(spyFs);
    doReturn(false).when(spyFs).delete(eventPath, false);
    doCallRealMethod().when(spyFs).delete(val1Path, true);
    service.cleanUp(now);

    exists(eventPath);
    deleted(val1Path);
    verify(legacyReplicationPathRepository, never()).delete(cleanUpPath1);

    reset(legacyReplicationPathRepository);
    when(legacyReplicationPathRepository.findByCreationTimestampLessThanEqual(now.getMillis()))
        .thenReturn(Arrays.asList(cleanUpPath1));
    doCallRealMethod().when(spyFs).delete(eventPath, false);
    service.cleanUp(now);
    deleted(eventPath);
    verify(legacyReplicationPathRepository).delete(cleanUpPath1);
  }

  @Test
  public void eventuallyConsistentCleanUpRemainingPartition() throws Exception {
    when(legacyReplicationPathRepository.findByCreationTimestampLessThanEqual(now.getMillis()))
        .thenReturn(Arrays.asList(cleanUpPath1, cleanUpPath2));

    doReturn(false).when(spyFs).delete(val1Path, true);
    service.cleanUp(now);

    exists(val1Path);
    deleted(val2Path);
    verify(legacyReplicationPathRepository, never()).delete(cleanUpPath1);
    verify(legacyReplicationPathRepository).delete(cleanUpPath2);

    reset(legacyReplicationPathRepository);
    when(legacyReplicationPathRepository.findByCreationTimestampLessThanEqual(now.getMillis()))
        .thenReturn(Arrays.asList(cleanUpPath1));
    doReturn(false).when(spyFs).delete(eventPath, false);
    doCallRealMethod().when(spyFs).delete(val1Path, true);
    service.cleanUp(now);

    exists(eventPath);
    deleted(val1Path);
    verify(legacyReplicationPathRepository).delete(cleanUpPath1);

    reset(legacyReplicationPathRepository);
    // return empty list as val3Parh is still in use.
    when(legacyReplicationPathRepository.findByCreationTimestampLessThanEqual(now.getMillis()))
        .thenReturn(Collections.<LegacyReplicaPath> emptyList());
    service.cleanUp(now);
    verify(legacyReplicationPathRepository, never()).delete(any(LegacyReplicaPath.class));
    exists(eventPath, val3Path);
  }

  @Test
  public void eventuallyConsistentCleanUpOnlyKeys() throws Exception {
    Path val4Path = new Path(tmpFolder.newFolder("foo", "bar", PATH_EVENT_ID, "test=2", "val=4").getCanonicalPath());
    LegacyReplicaPath cleanUpPath4 = new LegacyReplicaPath(EVENT_ID, PATH_EVENT_ID, val4Path.toString());

    when(legacyReplicationPathRepository.findByCreationTimestampLessThanEqual(now.getMillis()))
        .thenReturn(Arrays.asList(cleanUpPath4));

    doCallRealMethod().when(spyFs).delete(val4Path, true);
    doReturn(false).when(spyFs).exists(val4Path);
    doReturn(false).when(spyFs).exists(eventPath);
    service.cleanUp(now);

    deleted(val4Path);
    verify(legacyReplicationPathRepository).delete(cleanUpPath4);
  }

  @Test
  public void cleanUpNonEmptyParent() throws Exception {
    when(legacyReplicationPathRepository.findByCreationTimestampLessThanEqual(now.getMillis()))
        .thenReturn(Arrays.asList(cleanUpPath1, cleanUpPath2));

    service.cleanUp(now);

    verify(legacyReplicationPathRepository).delete(cleanUpPath1);
    verify(legacyReplicationPathRepository).delete(cleanUpPath2);
    exists(val3Path);
    deleted(val1Path, val2Path);
  }

  @Test
  public void onePathCannotBeDeleted() throws Exception {
    doThrow(new IOException()).when(spyFs).delete(val2Path, true);
    when(legacyReplicationPathRepository.findByCreationTimestampLessThanEqual(now.getMillis()))
        .thenReturn(Arrays.asList(cleanUpPath1, cleanUpPath2, cleanUpPath3));

    service.cleanUp(now);

    verify(legacyReplicationPathRepository).delete(cleanUpPath1);
    verify(legacyReplicationPathRepository, never()).delete(cleanUpPath2);
    verify(legacyReplicationPathRepository).delete(cleanUpPath3);
    exists(val2Path);
    deleted(val1Path, val3Path);
  }

  @Test
  public void filesystemParentPathDeletionFailsKeepsCleanUpPathForRetry() throws Exception {
    doThrow(new IOException("Can't delete parent!")).when(spyFs).delete(test1Path, false);
    when(legacyReplicationPathRepository.findByCreationTimestampLessThanEqual(now.getMillis()))
        .thenReturn(Arrays.asList(cleanUpPath1, cleanUpPath2, cleanUpPath3));

    service.cleanUp(now);

    verify(legacyReplicationPathRepository).delete(cleanUpPath1);
    verify(legacyReplicationPathRepository).delete(cleanUpPath2);
    // Need to keep cleanup path in DB so we can attempt to cleanup parent at a later stage.
    verify(legacyReplicationPathRepository, never()).delete(cleanUpPath3);
    exists(test1Path);
    deleted(val1Path, val2Path, val3Path);
  }

  @Test(expected = CircusTrainException.class)
  public void repositoryDeletionFails() throws Exception {
    doThrow(new RuntimeException()).when(legacyReplicationPathRepository).delete(cleanUpPath1);
    when(legacyReplicationPathRepository.findByCreationTimestampLessThanEqual(now.getMillis()))
        .thenReturn(Arrays.asList(cleanUpPath1));

    service.cleanUp(now);
  }

  @Test
  public void repositoryDeletionWithOptimisticLockingExceptionIsIgnored() throws Exception {
    doThrow(new ObjectOptimisticLockingFailureException("Error", new Exception()))
        .when(legacyReplicationPathRepository)
        .delete(cleanUpPath1);
    when(legacyReplicationPathRepository.findByCreationTimestampLessThanEqual(now.getMillis()))
        .thenReturn(Arrays.asList(cleanUpPath1, cleanUpPath2));
    service.cleanUp(now);
    // all paths will still be deleted
    deleted(val1Path, val2Path);
  }

  // TODO remove this when there are no more records around that hit this.
  @Test
  public void regexp() {
    Pattern pattern = Pattern.compile(EventIdExtractor.EVENT_ID_REGEXP);
    assertThat(pattern.matcher("a/ctp-20160726T162136.657Z-Vdqln6v7").matches(), is(true));
    assertThat(pattern.matcher("a/ctt-20160726T162136.657Z-Vdqln6v7").matches(), is(true));
    assertThat(pattern.matcher("a/ctp-20000101T000000.000Z-Vdqln6v7").matches(), is(true));
    assertThat(pattern.matcher("a/ctp-20991231T235959.999Z-Vdqln6v7").matches(), is(true));

    assertThat(pattern.matcher("a/ctp-19990101T000000.000Z-Vdqln6v7").matches(), is(false));
    assertThat(pattern.matcher("a/cta-19990101T000000.000Z-Vdqln6v7").matches(), is(false));
    assertThat(pattern.matcher("a/ctp-20000101T000000.00Z-Vdqln6v7").matches(), is(false));
    assertThat(pattern.matcher("a/ctp-20000101T000000.0000Z-Vdqln6v7").matches(), is(false));
    assertThat(pattern.matcher("a/ctp-20000101T000000.000Z-dqln6v7").matches(), is(false));

    assertThat(pattern.matcher("a/ctp-20991231T235959.999Z-Vdqln6v78").matches(), is(false));
    assertThat(pattern.matcher("a/ctp-21001231T235959.999Z-Vdqln6v7").matches(), is(false));
    assertThat(pattern.matcher("a/ctp-20993231T235959.999Z-Vdqln6v7").matches(), is(false));
    assertThat(pattern.matcher("a/ctp-20991231T236959.999Z-Vdqln6v7").matches(), is(false));
    assertThat(pattern.matcher("a/ctp-20991231T235969.999Z-Vdqln6v7").matches(), is(false));

    Matcher matcher = pattern.matcher("s3://bucket-dj49488/ctt-20160726T162136.657Z-Vdqln6v7/part-00000");
    matcher.matches();
    assertThat(matcher.group(1), is("ctt-20160726T162136.657Z-Vdqln6v7"));

    matcher = pattern.matcher("s3://bucket-dj49488/ctp-20160726T162136.657Z-Vdqln6v7/2012/01/01/00/part-00000");
    matcher.matches();
    assertThat(matcher.group(1), is("ctp-20160726T162136.657Z-Vdqln6v7"));
  }

  private void deleted(Path... paths) {
    for (Path path : paths) {
      assertFalse(new File(path.toString()).exists());
    }
  }

  private void exists(Path... paths) {
    for (Path path : paths) {
      assertTrue(new File(path.toString()).exists());
    }
  }
}
