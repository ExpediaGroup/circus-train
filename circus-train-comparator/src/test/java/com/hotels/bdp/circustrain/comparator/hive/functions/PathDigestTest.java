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
package com.hotels.bdp.circustrain.comparator.hive.functions;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.net.URI;

import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.comparator.hive.functions.PathDigest;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.PathMetadata;

@RunWith(MockitoJUnitRunner.class)
public class PathDigestTest {

  private static final String BASE_64_REGEX = "^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$";

  private static final long LAST_MODIFIED = 123456789L;
  private static final String FILE_LOCATION = "file:/abc";
  private static final String MD5 = "MD5";

  private @Mock Path path;
  private @Mock FileChecksum checksum;

  private final PathDigest function = new PathDigest();

  @Test
  public void typical() throws Exception {
    when(path.toUri()).thenReturn(new URI(FILE_LOCATION));
    when(checksum.getAlgorithmName()).thenReturn(MD5);
    when(checksum.getLength()).thenReturn(1);
    when(checksum.getBytes()).thenReturn(new byte[] {});

    PathMetadata pathDescriptor = new PathMetadata(path, LAST_MODIFIED, checksum, ImmutableList.<PathMetadata> of());
    String base64Digest = function.apply(pathDescriptor);
    assertThat(base64Digest.matches(BASE_64_REGEX), is(true));
  }

  @Test
  public void noChecksum() throws Exception {
    when(path.toUri()).thenReturn(new URI(FILE_LOCATION));

    PathMetadata pathDescriptor = new PathMetadata(path, LAST_MODIFIED, checksum, ImmutableList.<PathMetadata> of());
    String base64Digest = function.apply(pathDescriptor);
    assertThat(base64Digest.matches(BASE_64_REGEX), is(true));
  }

  @Test(expected = CircusTrainException.class)
  public void unknownAlgorithm() throws Exception {
    new PathDigest("ABCBCBC");
  }

}
