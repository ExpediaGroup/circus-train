/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.hotels.bdp.circustrain.core.transformation;

import static org.mockito.Mockito.verify;

import java.util.ArrayList;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.metadata.PartitionTransformation;

@RunWith(MockitoJUnitRunner.class)
public class CompositePartitionTransformationTest {

  private CompositePartitionTransformation partitionTransformations;
  @Mock
  private PartitionTransformation transformationOne;
  @Mock
  private PartitionTransformation transformationTwo;

  private ArrayList<PartitionTransformation> transformations = new ArrayList<>();

  @Test
  public void transform() throws Exception {
    transformations.add(transformationOne);
    transformations.add(transformationTwo);

    partitionTransformations = new CompositePartitionTransformation(transformations);

    partitionTransformations.transform(null);

    verify(transformationOne).transform(null);
    verify(transformationTwo).transform(null);
  }
}
