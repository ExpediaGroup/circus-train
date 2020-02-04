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
package com.hotels.bdp.circustrain.metrics.conf;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.CircusTrainException;

@RunWith(MockitoJUnitRunner.class)
public class GraphiteValidatorTest {

  private @Mock GraphiteLoader loader;
  private final Graphite input = new Graphite();
  private final Graphite loaded = new Graphite();

  private GraphiteValidator validator;

  @Before
  public void before() {
    when(loader.load(any(Path.class))).thenReturn(loaded);

    validator = new GraphiteValidator(loader);
  }

  @Test
  public void nullGraphite() {
    ValidatedGraphite validated = validator.validate(null);

    assertThat(validated.isEnabled(), is(false));
  }

  @Test
  public void disabledGraphite() {
    input.init();
    ValidatedGraphite validated = validator.validate(input);

    assertThat(validated.isEnabled(), is(false));
  }

  @Test
  public void enabledConfigOnly() {
    input.setConfig(new Path("."));
    input.init();

    ValidatedGraphite validated = validator.validate(input);

    assertThat(validated.isEnabled(), is(false));
  }

  @Test(expected = CircusTrainException.class)
  public void enabledNoHost() {
    input.setPrefix("prefix");
    input.setNamespace("namespace");
    input.init();

    validator.validate(input);
  }

  @Test(expected = CircusTrainException.class)
  public void enabledNoPrefix() {
    input.setHost("host");
    input.setNamespace("namespace");
    input.init();

    validator.validate(input);
  }

  @Test(expected = CircusTrainException.class)
  public void enabledNoNamespace() {
    input.setHost("host");
    input.setPrefix("prefix");
    input.init();

    validator.validate(input);
  }

  @Test
  public void enabledHostPrefixAndNamespace() {
    input.setHost("host");
    input.setPrefix("prefix");
    input.setNamespace("namespace");
    input.init();

    ValidatedGraphite validated = validator.validate(input);

    assertThat(validated.isEnabled(), is(true));
    assertThat(validated.getHost(), is("host"));
    assertThat(validated.getFormattedPrefix(), is("prefix.namespace"));
  }

  @Test
  public void enabledHostPrefixAndNamespaceFromConfigFile() {
    loaded.setHost("hostx");
    loaded.setPrefix("prefixx");
    loaded.setNamespace("namespacex");
    loaded.init();

    ValidatedGraphite validated = validator.validate(input);

    assertThat(validated.isEnabled(), is(true));
    assertThat(validated.getHost(), is("hostx"));
    assertThat(validated.getFormattedPrefix(), is("prefixx.namespacex"));
  }

  @Test
  public void enabledHostPrefixAndNamespaceFromConfigFileWithOverride() {
    loaded.setHost("hostx");
    loaded.setPrefix("prefixx");
    loaded.setNamespace("namespacex");
    loaded.init();

    input.setHost("host");
    input.setPrefix("prefix");
    input.setNamespace("namespace");
    input.init();

    ValidatedGraphite validated = validator.validate(input);

    assertThat(validated.isEnabled(), is(true));
    assertThat(validated.getHost(), is("host"));
    assertThat(validated.getFormattedPrefix(), is("prefix.namespace"));
  }

}
