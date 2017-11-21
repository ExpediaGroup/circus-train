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
package com.hotels.bdp.circustrain.api.copier;

import static java.util.Arrays.asList;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CompositeCopierFactoryTest {

  private static class DummyCopierPathGenerator implements CopierPathGenerator {
    private final List<Path> sourcePaths;
    private final List<Path> replicaPaths;

    DummyCopierPathGenerator(List<Path> sourcePaths, List<Path> replicaPaths) {
      this.sourcePaths = sourcePaths;
      this.replicaPaths = replicaPaths;
    }

    @Override
    public Path generateSourceBaseLocation(CopierPathGeneratorParams copierParams) {
      return sourcePaths.get(copierParams.getCopierIndex());
    }

    @Override
    public Path generateReplicaLocation(CopierPathGeneratorParams copierParams) {
      return replicaPaths.get(copierParams.getCopierIndex());
    }
  }

  private @Mock CopierFactory firstCopierFactory;
  private @Mock Copier firstCopier;
  private @Mock CopierFactory secondCopierFactory;
  private @Mock Copier secondCopier;
  private @Mock Map<String, Object> overridingCopierOptions;

  @Before
  public void init() {
    doReturn(firstCopier).when(firstCopierFactory).newInstance(anyString(), any(Path.class),
        Matchers.<List<Path>> any(), any(Path.class), Matchers.<Map<String, Object>> any());
    doReturn(firstCopier).when(firstCopierFactory).newInstance(anyString(), any(Path.class), any(Path.class),
        Matchers.<Map<String, Object>> any());
    doReturn(secondCopier).when(secondCopierFactory).newInstance(anyString(), any(Path.class),
        Matchers.<List<Path>> any(), any(Path.class), Matchers.<Map<String, Object>> any());
    doReturn(secondCopier).when(secondCopierFactory).newInstance(anyString(), any(Path.class), any(Path.class),
        Matchers.<Map<String, Object>> any());
  }

  @Test
  public void copyFromPreviousCopierReplicaLocation() {
    Path sourceBaseLocation = new Path("source");
    List<Path> sourceSubLocations = Collections.emptyList();
    Path firstReplicaLocation = new Path("first");
    Path secondReplicaLocation = new Path("second");
    CompositeCopierFactory copierFactory = new CompositeCopierFactory(
        Arrays.asList(firstCopierFactory, secondCopierFactory),
        new DummyCopierPathGenerator(asList(sourceBaseLocation, firstReplicaLocation),
            asList(firstReplicaLocation, secondReplicaLocation)),
        MetricsMerger.DEFAULT);

    copierFactory.newInstance("eventId", sourceBaseLocation, sourceSubLocations, new Path("replicaLocation"),
        overridingCopierOptions);
    verify(firstCopierFactory).newInstance("eventId", sourceBaseLocation, sourceSubLocations, firstReplicaLocation,
        overridingCopierOptions);
    verify(secondCopierFactory).newInstance("eventId", firstReplicaLocation, sourceSubLocations, secondReplicaLocation,
        overridingCopierOptions);
  }

  @Test
  public void copyFromPreviousCopierReplicaLocationNoSubLocations() {
    Path sourceBaseLocation = new Path("source");
    Path firstReplicaLocation = new Path("first");
    Path secondReplicaLocation = new Path("second");
    CompositeCopierFactory copierFactory = new CompositeCopierFactory(
        Arrays.asList(firstCopierFactory, secondCopierFactory),
        new DummyCopierPathGenerator(asList(sourceBaseLocation, firstReplicaLocation),
            asList(firstReplicaLocation, secondReplicaLocation)),
        MetricsMerger.DEFAULT);

    copierFactory.newInstance("eventId", sourceBaseLocation, new Path("replicaLocation"), overridingCopierOptions);
    verify(firstCopierFactory).newInstance("eventId", sourceBaseLocation, firstReplicaLocation,
        overridingCopierOptions);
    verify(secondCopierFactory).newInstance("eventId", firstReplicaLocation, secondReplicaLocation,
        overridingCopierOptions);
  }

  @Test
  public void copyFromOriginalSourceToMultipleReplicaLocations() {
    Path sourceBaseLocation = new Path("source");
    List<Path> sourceSubLocations = Collections.emptyList();
    Path firstReplicaLocation = new Path("first");
    Path secondReplicaLocation = new Path("second");
    CompositeCopierFactory copierFactory = new CompositeCopierFactory(
        Arrays.asList(firstCopierFactory, secondCopierFactory),
        new DummyCopierPathGenerator(asList(sourceBaseLocation, sourceBaseLocation),
            asList(firstReplicaLocation, secondReplicaLocation)),
        MetricsMerger.DEFAULT);

    copierFactory.newInstance("eventId", sourceBaseLocation, sourceSubLocations, new Path("replicaLocation"),
        overridingCopierOptions);
    verify(firstCopierFactory).newInstance("eventId", sourceBaseLocation, sourceSubLocations, firstReplicaLocation,
        overridingCopierOptions);
    verify(secondCopierFactory).newInstance("eventId", sourceBaseLocation, sourceSubLocations, secondReplicaLocation,
        overridingCopierOptions);
  }

  @Test
  public void copyFromOriginalSourceToMultipleReplicaLocationsNoSubLocations() {
    Path sourceBaseLocation = new Path("source");
    Path firstReplicaLocation = new Path("first");
    Path secondReplicaLocation = new Path("second");
    CompositeCopierFactory copierFactory = new CompositeCopierFactory(
        Arrays.asList(firstCopierFactory, secondCopierFactory),
        new DummyCopierPathGenerator(asList(sourceBaseLocation, sourceBaseLocation),
            asList(firstReplicaLocation, secondReplicaLocation)),
        MetricsMerger.DEFAULT);

    copierFactory.newInstance("eventId", sourceBaseLocation, new Path("replicaLocation"), overridingCopierOptions);
    verify(firstCopierFactory).newInstance("eventId", sourceBaseLocation, firstReplicaLocation,
        overridingCopierOptions);
    verify(secondCopierFactory).newInstance("eventId", sourceBaseLocation, secondReplicaLocation,
        overridingCopierOptions);
  }

}
