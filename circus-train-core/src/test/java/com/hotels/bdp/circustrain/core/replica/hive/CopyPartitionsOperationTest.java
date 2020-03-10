package com.hotels.bdp.circustrain.core.replica.hive;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@RunWith(MockitoJUnitRunner.class)
public class CopyPartitionsOperationTest {

  private static final short BATCH_SIZE = 2;
  private static final String OLD_TABLE_NAME = "old_table";
  private static final String NEW_TABLE_NAME = "new_table";
  private static final String OLD_DB_NAME = "old_db";
  private static final String NEW_DB_NAME = "new_db";

  private @Mock CloseableMetaStoreClient client;
  private @Captor ArgumentCaptor<List<Partition>> partitionCaptor;
  private Table oldTable = new Table();
  private Table newTable = new Table();
  private CopyPartitionsOperation operation;

  @Before
  public void setUp() {
    operation = new CopyPartitionsOperation(BATCH_SIZE);
    oldTable.setTableName(OLD_TABLE_NAME);
    newTable.setTableName(NEW_TABLE_NAME);
    oldTable.setDbName(OLD_DB_NAME);
    newTable.setDbName(NEW_DB_NAME);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(Arrays.asList(new FieldSchema("id", "bigint", "")));
    newTable.setSd(sd);
  }

  @Test
  public void typicalCopyPartitions() throws Exception {
    List<String> partitionNames = Arrays.asList("part1");
    when(client.listPartitionNames(OLD_DB_NAME, OLD_TABLE_NAME, (short) -1))
        .thenReturn(partitionNames);
    when(client.getPartitionsByNames(OLD_DB_NAME, OLD_TABLE_NAME, Arrays.asList("part1")))
        .thenReturn(createPartitions(1));

    operation.execute(client, oldTable, newTable);

    verify(client, times(1)).add_partitions(partitionCaptor.capture());
    List<List<Partition>> captured = partitionCaptor.getAllValues();
    assertThat(captured.size(), is(1));
    assertThat(captured.get(0).size(), is(1));
  }

  @Test
  public void singleBatch() throws Exception {
    List<String> partitionNames = Arrays.asList("part1", "part2");
    when(client.listPartitionNames(OLD_DB_NAME, OLD_TABLE_NAME, (short) -1))
        .thenReturn(partitionNames);
    when(client.getPartitionsByNames(OLD_DB_NAME, OLD_TABLE_NAME, Arrays.asList("part1", "part2")))
        .thenReturn(createPartitions(2));

    operation.execute(client, oldTable, newTable);

    verify(client, times(1)).add_partitions(partitionCaptor.capture());
    List<List<Partition>> captured = partitionCaptor.getAllValues();
    assertThat(captured.size(), is(1));
    assertThat(captured.get(0).size(), is(2));
  }

  @Test
  public void multipleBatches() throws Exception {
    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");
    when(client.listPartitionNames(OLD_DB_NAME, OLD_TABLE_NAME, (short) -1))
        .thenReturn(partitionNames);
    when(client.getPartitionsByNames(OLD_DB_NAME, OLD_TABLE_NAME, Arrays.asList("part1", "part2")))
        .thenReturn(createPartitions(2));
    when(client.getPartitionsByNames(OLD_DB_NAME, OLD_TABLE_NAME, Arrays.asList("part3")))
        .thenReturn(createPartitions(1));

    operation.execute(client, oldTable, newTable);

    verify(client, times(2)).add_partitions(partitionCaptor.capture());
    List<List<Partition>> captured = partitionCaptor.getAllValues();
    assertThat(captured.size(), is(2));
    assertThat(captured.get(0).size(), is(2));
    assertThat(captured.get(1).size(), is(1));
  }

  @Test
  public void noPartitions() throws Exception {
    when(client.listPartitionNames(OLD_DB_NAME, OLD_TABLE_NAME, (short) -1))
        .thenReturn(Collections.emptyList());
    when(client.getPartitionsByNames(OLD_DB_NAME, OLD_TABLE_NAME, Collections.emptyList()))
        .thenReturn(Collections.emptyList());

    operation.execute(client, oldTable, newTable);

    verify(client).listPartitionNames(OLD_DB_NAME, OLD_TABLE_NAME, (short) -1);
    verify(client).getPartitionsByNames(OLD_DB_NAME, OLD_TABLE_NAME, Collections.emptyList());
    verifyNoMoreInteractions(client);
  }

  private List<Partition> createPartitions(int count) {
    ArrayList<Partition> partitions = new ArrayList<>();
    for (int i=0; i < count; i++) {
      Partition partition = new Partition();
      partition.setSd(new StorageDescriptor());
      partitions.add(partition);
    }
    return partitions;
  }

}
