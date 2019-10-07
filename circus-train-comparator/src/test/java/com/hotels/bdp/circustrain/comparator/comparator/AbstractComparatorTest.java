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
package com.hotels.bdp.circustrain.comparator.comparator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.comparator.ComparatorRegistry;
import com.hotels.bdp.circustrain.comparator.api.ComparatorType;
import com.hotels.bdp.circustrain.comparator.api.Diff;

@RunWith(MockitoJUnitRunner.class)
public class AbstractComparatorTest {

  private static class EqualObject {
    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (!obj.getClass().equals(EqualObject.class)) {
        return false;
      }
      return true;
    }
  }

  private @Mock ComparatorRegistry comparatorRegistry;

  private final AbstractComparator<EqualObject, EqualObject> comparator = new AbstractComparator<EqualObject, EqualObject>(
      comparatorRegistry, ComparatorType.FULL_COMPARISON) {
    @Override
    public List<Diff<EqualObject, EqualObject>> compare(EqualObject a, EqualObject b) {
      return null;
    }
  };

  @Test
  public void checkForInequalityOfSameObject() {
    EqualObject object = new EqualObject();
    assertThat(comparator.checkForInequality(object, object), is(false));
  }

  @Test
  public void checkForInequalityLeftNull() {
    assertThat(comparator.checkForInequality(null, new EqualObject()), is(true));
  }

  @Test
  public void checkForInequalityRightNull() {
    assertThat(comparator.checkForInequality(new EqualObject(), null), is(true));
  }

  @Test
  public void checkForInequalityDifferentObject() {
    Object objectA = new Object();
    Object objectB = new Object();
    assertThat(comparator.checkForInequality(objectA, objectB), is(true));
  }

  @Test
  public void checkForInequalityDifferentObjectEquals() {
    EqualObject objectA = new EqualObject();
    EqualObject objectB = new EqualObject();
    assertThat(comparator.checkForInequality(objectA, objectB), is(false));
  }

}
