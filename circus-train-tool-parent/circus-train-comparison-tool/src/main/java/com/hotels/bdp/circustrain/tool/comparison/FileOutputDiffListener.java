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
package com.hotels.bdp.circustrain.tool.comparison;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.metastore.api.Partition;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.io.Files;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.comparator.api.Diff;
import com.hotels.bdp.circustrain.comparator.api.DiffListener;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.TableAndMetadata;

public class FileOutputDiffListener implements DiffListener {

  private static final Joiner NEWLINE_TAB_JOINER = Joiner.on("\n\t");
  private static final Function<Diff<Object, Object>, String> DIFF_TO_STRING = new Function<Diff<Object, Object>, String>() {
    @Override
    public String apply(@Nonnull Diff<Object, Object> diff) {
      return new StringBuilder(diff.message())
          .append(": left=")
          .append(diff.left())
          .append(", right=")
          .append(diff.right())
          .toString();
    }
  };

  private final File file;
  private PrintStream out;

  /**
   * Reported differences will be logged to the file.
   */
  public FileOutputDiffListener(File file) {
    this.file = file;
  }

  @Override
  public void onDiffStart(TableAndMetadata source, Optional<TableAndMetadata> replica) {
    try {
      Files.touch(file);
      out = new PrintStream(file, StandardCharsets.UTF_8.name());
    } catch (IOException e) {
      throw new CircusTrainException(e.getMessage(), e);
    }
    out.println("=================================================================================================");
    out.printf("Starting diff on source table 'table=%s, location=%s' and replicate table 'table=%s, location=%s'",
        source.getSourceTable(), source.getSourceLocation(),
        replica.isPresent() ? replica.get().getSourceTable() : "null",
        replica.isPresent() ? replica.get().getSourceLocation() : "null");
    out.println();
  }

  @Override
  public void onChangedTable(List<Diff<Object, Object>> differences) {
    out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
    out.println("Table differences: ");
    out.print("\t");
    out.println(NEWLINE_TAB_JOINER.join(FluentIterable.from(differences).transform(DIFF_TO_STRING).toList()));
  }

  @Override
  public void onNewPartition(String partitionName, Partition partition) {
    out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
    out.println("New partition on source table (not in replica): ");
    out.println(partitionToString(partition));
  }

  @Override
  public void onChangedPartition(String partitionName, Partition partition, List<Diff<Object, Object>> differences) {
    out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
    out.println("Partition differs: ");
    out.println(partitionName);
    out.print("\t");
    out.println(NEWLINE_TAB_JOINER.join(FluentIterable.from(differences).transform(DIFF_TO_STRING).toList()));
  }

  @Override
  public void onDataChanged(String partitionName, Partition partition) {
    out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
    out.println("Data changed in partition: ");
    out.println(partitionToString(partition));
  }

  @Override
  public void onDiffEnd() {
    out.println("=================================================================================================");
    out.close();
  }

  private String partitionToString(Partition partition) {
    return "Partition values: " + partition.getValues();
  }

}
