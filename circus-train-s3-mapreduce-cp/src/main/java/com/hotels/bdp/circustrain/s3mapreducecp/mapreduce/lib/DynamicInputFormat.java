/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.mapred.lib.DynamicInputFormat} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/main/java/org/
 * apache/hadoop/tools/mapred/lib/DynamicInputFormat.java
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
package com.hotels.bdp.circustrain.s3mapreducecp.mapreduce.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.s3mapreducecp.CopyListingFileStatus;
import com.hotels.bdp.circustrain.s3mapreducecp.S3MapReduceCpConstants;
import com.hotels.bdp.circustrain.s3mapreducecp.util.ConfigurationUtil;

/**
 * DynamicInputFormat implements the "Worker pattern" for S3MapReduceCp. Rather than to split up the copy-list into a
 * set of static splits, the DynamicInputFormat does the following: 1. Splits the copy-list into small chunks on the
 * DFS. 2. Creates a set of empty "dynamic" splits, that each consume as many chunks as it can. This arrangement ensures
 * that a single slow mapper won't slow down the entire job (since the slack will be picked up by other mappers, who
 * consume more chunks.) By varying the split-ratio, one can vary chunk sizes to achieve different performance
 * characteristics.
 */
public class DynamicInputFormat<K, V> extends InputFormat<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicInputFormat.class);

  private static final String CONF_LABEL_LISTING_SPLIT_RATIO = "mapred.listing.split.ratio";
  private static final String CONF_LABEL_NUM_SPLITS = "mapred.num.splits";
  private static final String CONF_LABEL_NUM_ENTRIES_PER_CHUNK = "mapred.num.entries.per.chunk";
  private static final int N_CHUNKS_OPEN_AT_ONCE_DEFAULT = 16;

  /**
   * Implementation of InputFormat::getSplits(). This method splits up the copy-listing file into chunks, and assigns
   * the first batch to different tasks.
   *
   * @param jobContext JobContext for the map job.
   * @return The list of (empty) dynamic input-splits.
   * @throws IOException, on failure.
   * @throws InterruptedException
   */
  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    LOG.info("DynamicInputFormat: Getting splits for job: {}", jobContext.getJobID());
    return createSplits(jobContext, splitCopyListingIntoChunksWithShuffle(jobContext));
  }

  private List<InputSplit> createSplits(JobContext jobContext, List<DynamicInputChunk> chunks) throws IOException {
    int numMaps = getNumMapTasks(jobContext.getConfiguration());

    final int nSplits = Math.min(numMaps, chunks.size());
    List<InputSplit> splits = new ArrayList<>(nSplits);

    for (int i = 0; i < nSplits; ++i) {
      TaskID taskId = new TaskID(jobContext.getJobID(), TaskType.MAP, i);
      chunks.get(i).assignTo(taskId);
      splits.add(new FileSplit(chunks.get(i).getPath(), 0,
          // Setting non-zero length for FileSplit size, to avoid a possible
          // future when 0-sized file-splits are considered "empty" and skipped
          // over.
          getMinRecordsPerChunk(jobContext.getConfiguration()), null));
    }
    ConfigurationUtil.publish(jobContext.getConfiguration(), CONF_LABEL_NUM_SPLITS, splits.size());
    return splits;
  }

  private List<DynamicInputChunk> splitCopyListingIntoChunksWithShuffle(JobContext context) throws IOException {

    final Configuration configuration = context.getConfiguration();
    int numRecords = getNumberOfRecords(configuration);
    int numMaps = getNumMapTasks(configuration);
    int maxChunksTolerable = getMaxChunksTolerable(configuration);

    // Number of chunks each map will process, on average.
    int splitRatio = getListingSplitRatio(configuration, numMaps, numRecords);
    validateNumChunksUsing(splitRatio, numMaps, maxChunksTolerable);

    int numEntriesPerChunk = (int) Math.ceil((float) numRecords / (splitRatio * numMaps));
    ConfigurationUtil.publish(context.getConfiguration(), CONF_LABEL_NUM_ENTRIES_PER_CHUNK, numEntriesPerChunk);

    final int nChunksTotal = (int) Math.ceil((float) numRecords / numEntriesPerChunk);
    int nChunksOpenAtOnce = Math.min(N_CHUNKS_OPEN_AT_ONCE_DEFAULT, nChunksTotal);

    Path listingPath = getListingFilePath(configuration);
    SequenceFile.Reader reader = new SequenceFile.Reader(configuration, SequenceFile.Reader.file(listingPath));

    List<DynamicInputChunk> openChunks = new ArrayList<>();

    List<DynamicInputChunk> chunksFinal = new ArrayList<>();

    CopyListingFileStatus fileStatus = new CopyListingFileStatus();
    Text relPath = new Text();
    int recordCounter = 0;
    int chunkCount = 0;

    try {

      while (reader.next(relPath, fileStatus)) {
        if (recordCounter % (nChunksOpenAtOnce * numEntriesPerChunk) == 0) {
          // All chunks full. Create new chunk-set.
          closeAll(openChunks);
          chunksFinal.addAll(openChunks);

          openChunks = createChunks(configuration, chunkCount, nChunksTotal, nChunksOpenAtOnce);

          chunkCount += openChunks.size();

          nChunksOpenAtOnce = openChunks.size();
          recordCounter = 0;
        }

        // Shuffle into open chunks.
        openChunks.get(recordCounter % nChunksOpenAtOnce).write(relPath, fileStatus);
        ++recordCounter;
      }

    } finally {
      closeAll(openChunks);
      chunksFinal.addAll(openChunks);
      IOUtils.closeStream(reader);
    }

    LOG.info("Number of dynamic-chunk-files created: {}", chunksFinal.size());
    return chunksFinal;
  }

  private static void validateNumChunksUsing(int splitRatio, int numMaps, int maxChunksTolerable) throws IOException {
    if (splitRatio * numMaps > maxChunksTolerable) {
      throw new IOException("Too many chunks created with splitRatio:"
          + splitRatio
          + ", numMaps:"
          + numMaps
          + ". Reduce numMaps or decrease split-ratio to proceed.");
    }
  }

  private static void closeAll(List<DynamicInputChunk> chunks) {
    for (DynamicInputChunk chunk : chunks) {
      chunk.close();
    }
  }

  private static List<DynamicInputChunk> createChunks(
      Configuration config,
      int chunkCount,
      int nChunksTotal,
      int nChunksOpenAtOnce)
    throws IOException {
    List<DynamicInputChunk> chunks = new ArrayList<>();
    int chunkIdUpperBound = Math.min(nChunksTotal, chunkCount + nChunksOpenAtOnce);

    // If there will be fewer than nChunksOpenAtOnce chunks left after
    // the current batch of chunks, fold the remaining chunks into
    // the current batch.
    if (nChunksTotal - chunkIdUpperBound < nChunksOpenAtOnce) {
      chunkIdUpperBound = nChunksTotal;
    }

    for (int i = chunkCount; i < chunkIdUpperBound; ++i) {
      chunks.add(createChunk(i, config));
    }
    return chunks;
  }

  private static DynamicInputChunk createChunk(int chunkId, Configuration config) throws IOException {
    return DynamicInputChunk.createChunkForWrite(String.format("%05d", chunkId), config);
  }

  private static Path getListingFilePath(Configuration configuration) {
    String listingFilePathString = configuration.get(S3MapReduceCpConstants.CONF_LABEL_LISTING_FILE_PATH, "");

    assert !"".equals(listingFilePathString) : "Listing file not found.";

    Path listingFilePath = new Path(listingFilePathString);
    try {
      assert listingFilePath.getFileSystem(configuration).exists(listingFilePath) : "Listing file: "
          + listingFilePath
          + " not found.";
    } catch (IOException e) {
      assert false : "Listing file: " + listingFilePath + " couldn't be accessed. " + e.getMessage();
    }
    return listingFilePath;
  }

  private static int getNumberOfRecords(Configuration configuration) {
    return ConfigurationUtil.getInt(configuration, S3MapReduceCpConstants.CONF_LABEL_TOTAL_NUMBER_OF_RECORDS);
  }

  private static int getNumMapTasks(Configuration configuration) {
    return ConfigurationUtil.getInt(configuration, MRJobConfig.NUM_MAPS);
  }

  private static int getListingSplitRatio(Configuration configuration, int numMaps, int numPaths) {
    return configuration.getInt(CONF_LABEL_LISTING_SPLIT_RATIO, getSplitRatio(numMaps, numPaths, configuration));
  }

  private static int getMaxChunksTolerable(Configuration conf) {
    int maxChunksTolerable = conf.getInt(S3MapReduceCpConstants.CONF_LABEL_MAX_CHUNKS_TOLERABLE,
        S3MapReduceCpConstants.MAX_CHUNKS_TOLERABLE_DEFAULT);
    if (maxChunksTolerable <= 0) {
      LOG.warn("{} should be positive. Fall back to default value: {}",
          S3MapReduceCpConstants.CONF_LABEL_MAX_CHUNKS_TOLERABLE, S3MapReduceCpConstants.MAX_CHUNKS_TOLERABLE_DEFAULT);
      maxChunksTolerable = S3MapReduceCpConstants.MAX_CHUNKS_TOLERABLE_DEFAULT;
    }
    return maxChunksTolerable;
  }

  private static int getMaxChunksIdeal(Configuration conf) {
    int maxChunksIdeal = conf.getInt(S3MapReduceCpConstants.CONF_LABEL_MAX_CHUNKS_IDEAL,
        S3MapReduceCpConstants.MAX_CHUNKS_IDEAL_DEFAULT);
    if (maxChunksIdeal <= 0) {
      LOG.warn("{} should be positive. Fall back to default value: {}",
          S3MapReduceCpConstants.CONF_LABEL_MAX_CHUNKS_IDEAL, S3MapReduceCpConstants.MAX_CHUNKS_IDEAL_DEFAULT);
      maxChunksIdeal = S3MapReduceCpConstants.MAX_CHUNKS_IDEAL_DEFAULT;
    }
    return maxChunksIdeal;
  }

  private static int getMinRecordsPerChunk(Configuration conf) {
    int minRecordsPerChunk = conf.getInt(S3MapReduceCpConstants.CONF_LABEL_MIN_RECORDS_PER_CHUNK,
        S3MapReduceCpConstants.MIN_RECORDS_PER_CHUNK_DEFAULT);
    if (minRecordsPerChunk <= 0) {
      LOG.warn("{} should be positive. Fall back to default value: {}",
          S3MapReduceCpConstants.CONF_LABEL_MIN_RECORDS_PER_CHUNK,
          S3MapReduceCpConstants.MIN_RECORDS_PER_CHUNK_DEFAULT);
      minRecordsPerChunk = S3MapReduceCpConstants.MIN_RECORDS_PER_CHUNK_DEFAULT;
    }
    return minRecordsPerChunk;
  }

  private static int getSplitRatio(Configuration conf) {
    int splitRatio = conf.getInt(S3MapReduceCpConstants.CONF_LABEL_SPLIT_RATIO,
        S3MapReduceCpConstants.SPLIT_RATIO_DEFAULT);
    if (splitRatio <= 0) {
      LOG.warn("{} should be positive. Fall back to default value: {}", S3MapReduceCpConstants.CONF_LABEL_SPLIT_RATIO,
          S3MapReduceCpConstants.SPLIT_RATIO_DEFAULT);
      splitRatio = S3MapReduceCpConstants.SPLIT_RATIO_DEFAULT;
    }
    return splitRatio;
  }

  /**
   * Package private, for testability.
   *
   * @param nMaps The number of maps requested for.
   * @param nRecords The number of records to be copied.
   * @return The number of splits each map should handle, ideally.
   */
  static int getSplitRatio(int nMaps, int nRecords) {
    return getSplitRatio(nMaps, nRecords, new Configuration());
  }

  /**
   * Package private, for testability.
   *
   * @param nMaps The number of maps requested for.
   * @param nRecords The number of records to be copied.
   * @param conf The configuration set by users.
   * @return The number of splits each map should handle, ideally.
   */
  static int getSplitRatio(int nMaps, int nRecords, Configuration conf) {
    int maxChunksIdeal = getMaxChunksIdeal(conf);
    int minRecordsPerChunk = getMinRecordsPerChunk(conf);
    int splitRatio = getSplitRatio(conf);

    if (nMaps == 1) {
      LOG.warn("nMaps == 1. Why use DynamicInputFormat?");
      return 1;
    }

    if (nMaps > maxChunksIdeal) {
      return splitRatio;
    }

    int nPickups = (int) Math.ceil((float) maxChunksIdeal / nMaps);
    int nRecordsPerChunk = (int) Math.ceil((float) nRecords / (nMaps * nPickups));

    return nRecordsPerChunk < minRecordsPerChunk ? splitRatio : nPickups;
  }

  static int getNumEntriesPerChunk(Configuration configuration) {
    return ConfigurationUtil.getInt(configuration, CONF_LABEL_NUM_ENTRIES_PER_CHUNK);
  }

  /**
   * Implementation of Inputformat::createRecordReader().
   *
   * @param inputSplit The split for which the RecordReader is required.
   * @param taskAttemptContext TaskAttemptContext for the current attempt.
   * @return DynamicRecordReader instance.
   * @throws IOException, on failure.
   * @throws InterruptedException
   */
  @Override
  public RecordReader<K, V> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
    return new DynamicRecordReader<>();
  }
}
