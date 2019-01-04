/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.bdp.circustrain.aws;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.amazonaws.services.s3.model.CannedAccessControlList;

public class CannedAclUtilTest {

  @Test
  public void toCannedAccessControlList() {
    assertThat(CannedAclUtils.toCannedAccessControlList("bucket-owner-full-control"), is(CannedAccessControlList.BucketOwnerFullControl));
  }

  @Test
  public void toNullCannedAccessControlList() {
    assertThat(CannedAclUtils.toCannedAccessControlList(null), is(nullValue()));
  }

  @Test
  public void toCannedAccessControlListCaseSensitiveness() {
    assertThat(CannedAclUtils.toCannedAccessControlList("bucket-owner-read"), is(CannedAccessControlList.BucketOwnerRead));
    assertThat(CannedAclUtils.toCannedAccessControlList("BUCKET-OWNER-READ"), is(CannedAccessControlList.BucketOwnerRead));
  }

  @Test(expected = IllegalArgumentException.class)
  public void toInvalidCannedAccessControlList() {
    CannedAclUtils.toCannedAccessControlList("all-read-write");
  }

}
