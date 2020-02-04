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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableList;

import com.hotels.bdp.circustrain.comparator.ComparatorRegistry;
import com.hotels.bdp.circustrain.comparator.api.BaseDiff;
import com.hotels.bdp.circustrain.comparator.api.Comparator;
import com.hotels.bdp.circustrain.comparator.api.ComparatorType;
import com.hotels.bdp.circustrain.comparator.api.Diff;

public class CollectionComparator extends AbstractComparator<Collection<?>, Object> {

  private final String property;

  public CollectionComparator(ComparatorRegistry comparatorRegistry, ComparatorType comparatorType) {
    this(comparatorRegistry, comparatorType, null);
  }

  public CollectionComparator(ComparatorRegistry comparatorRegistry, ComparatorType comparatorType, String property) {
    super(comparatorRegistry, comparatorType);
    this.property = property;
  }

  @Override
  public List<Diff<Object, Object>> compare(Collection<?> left, Collection<?> right) {
    if (left == null && right == null) {
      return ImmutableList.of();
    }

    if (left == null) {
      Diff<Object, Object> diff = new BaseDiff<Object, Object>(
          "Collection "
              + property
              + " of class "
              + right.getClass().getName()
              + " is different: left==null and right!=null",
          left, right);
      return ImmutableList.of(diff);
    }

    if (right == null) {
      Diff<Object, Object> diff = new BaseDiff<Object, Object>(
          "Collection "
              + property
              + " of class "
              + left.getClass().getName()
              + " is different: left!=null and right==null",
          left, right);
      return ImmutableList.of(diff);
    }

    List<Diff<Object, Object>> diffs = new ArrayList<>();
    if (left.size() != right.size()) {
      diffs.add(new BaseDiff<Object, Object>("Collection "
          + property
          + " of class "
          + left.getClass().getName()
          + " has different size: left.size()="
          + left.size()
          + " and right.size()="
          + right.size(), left, right));
    } else if (!left.isEmpty()) {
      Comparator<Object, Object> comparator = comparator(left.iterator().next().getClass());
      if (comparator != null) {
        int i = 0;
        Iterator<?> aIterator = left.iterator();
        Iterator<?> bIterator = right.iterator();
        while (carryOn(diffs) && aIterator.hasNext()) {
          Object aVal = aIterator.next();
          Object bVal = bIterator.next();
          List<Diff<Object, Object>> valDiffs = comparator.compare(aVal, bVal);
          for (Diff<Object, Object> diff : valDiffs) {
            diffs.add(new BaseDiff<>("Element "
                + i
                + " of collection "
                + property
                + " of class "
                + left.getClass().getName()
                + " is different: "
                + diff.message(), diff.left(), diff.right()));
          }
          ++i;
        }
      } else if (checkForInequality(left, right)) {
        diffs.add(new BaseDiff<Object, Object>(
            "Collection " + property + " of class " + left.getClass().getName() + " is different", left, right));
      }
    }
    return ImmutableList.copyOf(diffs);
  }

}
