/**
 * Copyright (C) 2016-2017 Expedia Inc and the original spring-integration contributors.
 *
 * The method PropertyComparator#get(T,String) is a copy of:
 *
 * https://github.com/spring-projects/spring-integration/blob/4.3.x/spring-integration-test/src/main/java/org/
 * springframework/integration/test/util/TestUtils.java#L61
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.springframework.beans.DirectFieldAccessor;

import com.google.common.collect.ImmutableList;

import com.hotels.bdp.circustrain.comparator.ComparatorRegistry;
import com.hotels.bdp.circustrain.comparator.api.BaseDiff;
import com.hotels.bdp.circustrain.comparator.api.Comparator;
import com.hotels.bdp.circustrain.comparator.api.ComparatorType;
import com.hotels.bdp.circustrain.comparator.api.Diff;

public class PropertyComparator<T> extends AbstractComparator<T, Object> {

  private final List<String> properties;

  public PropertyComparator(ComparatorType comparatorType, List<String> properties) {
    this(null, comparatorType, properties);
  }

  public PropertyComparator(
      ComparatorRegistry comparatorRegistry,
      ComparatorType comparatorType,
      List<String> properties) {
    super(comparatorRegistry, comparatorType);
    this.properties = properties;
  }

  @Override
  public List<Diff<Object, Object>> compare(T left, T right) {
    if (left == null && right == null) {
      return ImmutableList.of();
    }

    if (left == null) {
      Diff<Object, Object> diff = new BaseDiff<Object, Object>(
          "Left " + right.getClass().getName() + " is null, right is not null", left, right);
      return ImmutableList.of(diff);
    }

    if (right == null) {
      Diff<Object, Object> diff = new BaseDiff<Object, Object>(
          "Right " + left.getClass().getName() + " is null, left is not null", left, right);
      return ImmutableList.of(diff);
    }

    List<Diff<Object, Object>> diffs = new ArrayList<>(properties.size() + 1);
    Iterator<String> propertyIterator = properties.iterator();
    while (carryOn(diffs) && propertyIterator.hasNext()) {
      String property = propertyIterator.next();
      Object aVal = get(left, property);
      Object bVal = get(right, property);
      if (areCollections(aVal, bVal)) {
        CollectionComparator collectionComparator = new CollectionComparator(comparatorRegistry(), comparatorType(),
            property);
        diffs.addAll(collectionComparator.compare((Collection<?>) aVal, (Collection<?>) bVal));
      } else {
        Comparator<Object, Object> comparator = null;
        if (aVal != null) {
          comparator = comparator(aVal.getClass());
        }
        if (comparator != null) {
          diffs.addAll(comparator.compare(aVal, bVal));
        } else {
          if (checkForInequality(aVal, bVal)) {
            diffs.add(new BaseDiff<>(
                "Property " + property + " of class " + left.getClass().getName() + " is different", aVal, bVal));
          }
        }
      }
    }

    return ImmutableList.copyOf(diffs);
  }

  private boolean areCollections(Object aVal, Object bVal) {
    return aVal != null
        && bVal != null
        && Iterable.class.isAssignableFrom(aVal.getClass())
        && Iterable.class.isAssignableFrom(bVal.getClass());
  }

  private Object get(T object, String propertyPath) {
    Object value = null;
    DirectFieldAccessor accessor = new DirectFieldAccessor(object);
    String[] tokens = propertyPath.split("\\.");
    for (int i = 0; i < tokens.length; i++) {
      value = accessor.getPropertyValue(tokens[i]);
      if (value != null) {
        accessor = new DirectFieldAccessor(value);
      } else if (i == tokens.length - 1) {
        return null;
      } else {
        throw new IllegalArgumentException("Intermediate property '" + tokens[i] + "' is null");
      }
    }
    return value;
  }

}
