/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

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
    doReturn(firstCopier).when(firstCopierFactory).newInstance(any(CopierContext.class));
    doReturn(secondCopier).when(secondCopierFactory).newInstance(any(CopierContext.class));
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

    CopierContext copierContext = new CopierContext("eventId", sourceBaseLocation, sourceSubLocations,
        new Path("replicaLocation"), overridingCopierOptions);
    copierFactory.newInstance(copierContext);

    ArgumentCaptor<CopierContext> argument = ArgumentCaptor.forClass(CopierContext.class);
    verify(firstCopierFactory).newInstance(argument.capture());
    CopierContext captured = argument.getValue();
    assertThat(captured.getSourceBaseLocation(), is(sourceBaseLocation));
    assertThat(captured.getSourceSubLocations(), is(sourceSubLocations));
    assertThat(captured.getReplicaLocation(), is(firstReplicaLocation));

    verify(secondCopierFactory).newInstance(argument.capture());
    captured = argument.getValue();
    assertThat(captured.getSourceBaseLocation(), is(firstReplicaLocation));
    assertThat(captured.getSourceSubLocations(), is(sourceSubLocations));
    assertThat(captured.getReplicaLocation(), is(secondReplicaLocation));
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

    CopierContext copierContext = new CopierContext("eventId", sourceBaseLocation, new Path("replicaLocation"), overridingCopierOptions);
    copierFactory.newInstance(copierContext);

    ArgumentCaptor<CopierContext> argument = ArgumentCaptor.forClass(CopierContext.class);
    verify(firstCopierFactory).newInstance(argument.capture());
    CopierContext captured = argument.getValue();
    assertThat(captured.getSourceBaseLocation(), is(sourceBaseLocation));
    assertThat(captured.getReplicaLocation(), is(firstReplicaLocation));

    verify(secondCopierFactory).newInstance(argument.capture());
    captured = argument.getValue();
    assertThat(captured.getSourceBaseLocation(), is(firstReplicaLocation));
    assertThat(captured.getReplicaLocation(), is(secondReplicaLocation));
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
    
    CopierContext copierContext = new CopierContext("eventId", sourceBaseLocation, sourceSubLocations,
        new Path("replicaLocation"), overridingCopierOptions);
    copierFactory.newInstance(copierContext);

    ArgumentCaptor<CopierContext> argument = ArgumentCaptor.forClass(CopierContext.class);
    verify(firstCopierFactory).newInstance(argument.capture());
    CopierContext captured = argument.getValue();
    assertThat(captured.getSourceBaseLocation(), is(sourceBaseLocation));
    assertThat(captured.getSourceSubLocations(), is(sourceSubLocations));
    assertThat(captured.getReplicaLocation(), is(firstReplicaLocation));

    verify(secondCopierFactory).newInstance(argument.capture());
    captured = argument.getValue();
    assertThat(captured.getSourceBaseLocation(), is(sourceBaseLocation));
    assertThat(captured.getSourceSubLocations(), is(sourceSubLocations));
    assertThat(captured.getReplicaLocation(), is(secondReplicaLocation));
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
    CopierContext copierContext = new CopierContext("eventId", sourceBaseLocation, new Path("replicaLocation"),
        overridingCopierOptions);
    copierFactory.newInstance(copierContext);

    ArgumentCaptor<CopierContext> argument = ArgumentCaptor.forClass(CopierContext.class);
    verify(firstCopierFactory).newInstance(argument.capture());
    CopierContext captured = argument.getValue();
    assertThat(captured.getSourceBaseLocation(), is(sourceBaseLocation));
    assertThat(captured.getReplicaLocation(), is(firstReplicaLocation));

    verify(secondCopierFactory).newInstance(argument.capture());
    captured = argument.getValue();
    assertThat(captured.getSourceBaseLocation(), is(sourceBaseLocation));
    assertThat(captured.getReplicaLocation(), is(secondReplicaLocation));
  }

}
