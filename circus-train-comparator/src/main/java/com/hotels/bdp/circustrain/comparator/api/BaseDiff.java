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
package com.hotels.bdp.circustrain.comparator.api;

import com.google.common.base.Objects;

public class BaseDiff<LEFT, RIGHT> implements Diff<LEFT, RIGHT> {
  private final String message;
  private final LEFT left;
  private final RIGHT right;

  public BaseDiff(String message, LEFT left, RIGHT right) {
    this.message = message;
    this.left = left;
    this.right = right;
  }

  @Override
  public String message() {
    return message;
  }

  @Override
  public LEFT left() {
    return left;
  }

  @Override
  public RIGHT right() {
    return right;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("message", message).add("left", left).add("right", right).toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(message, left, right);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    BaseDiff<?, ?> other = (BaseDiff<?, ?>) obj;
    return Objects.equal(message, other.message)
        && Objects.equal(left, other.left)
        && Objects.equal(right, other.right);
  }

}
