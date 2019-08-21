package com.hotels.bdp.circustrain.core.annotation;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BeekeeperTableAnnotatorTest {

  private Table table = new Table();
  private BeekeeperTableAnnotator defaultTableAnnotator = new BeekeeperTableAnnotator();

  @Test
  public void typical() throws TException {
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    defaultTableAnnotator.annotateTable(table, properties);
    assertThat(table.getParameters().get("key"), is("value"));
    assertThat(table.getParameters().get(BeekeeperTableAnnotator.BEEKEEPER_PARAM_KEY), is("true"));
  }

}
