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
package com.hotels.bdp.circustrain.comparator.comparator;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;

import com.hotels.bdp.circustrain.comparator.ComparatorRegistry;
import com.hotels.bdp.circustrain.comparator.api.Comparator;
import com.hotels.bdp.circustrain.comparator.api.ComparatorType;
import com.hotels.bdp.circustrain.comparator.api.Diff;

public abstract class AbstractComparator<T, D> implements Comparator<T, D> {

  private final ComparatorRegistry comparatorRegistry;
  private final ComparatorType comparatorType;

  public AbstractComparator(ComparatorRegistry comparatorRegistry, ComparatorType comparatorType) {
    checkArgument(comparatorType != null, "comparatorType is required");
    this.comparatorRegistry = comparatorRegistry;
    this.comparatorType = comparatorType;
  }

  protected ComparatorRegistry comparatorRegistry() {
    return comparatorRegistry;
  }

  protected ComparatorType comparatorType() {
    return comparatorType;
  }

  protected boolean carryOn(List<Diff<Object, Object>> diffs) {
    return comparatorType.isFullComparison() || diffs.size() == 0;
  }

  protected boolean checkForInequality(Object left, Object right) {
    if (left == right) {
      return false;
    }
    if (left == null) {
      return true;
    }
    return !left.equals(right);
  }

  @SuppressWarnings("unchecked")
  protected Comparator<Object, Object> comparator(Class<?> type) {
    return comparatorRegistry == null ? null : (Comparator<Object, Object>) comparatorRegistry.comparatorFor(type);
  }

}
