/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.DistCpConstants} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/main/java/org/
 * apache/hadoop/tools/DistCpConstants.java
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
package com.hotels.bdp.circustrain.s3mapreducecp;

/**
 * Utility class to hold commonly used constants.
 */
public class S3MapReduceCpConstants {

  /* Default number of maps to use for S3MapReduceCp */
  public static final int DEFAULT_MAPS = 20;

  /* Default number of upload workers to use for each S3MapReduceCp Map */
  public static final int DEFAULT_NUM_OF_UPLOAD_WORKERS = 20;

  /* Default bandwidth if none specified */
  public static final int DEFAULT_BANDWIDTH_MB = 100;

  /*
   * Default strategy for copying. Implementation looked up from s3mapreducecp-default.xml
   */
  public static final String UNIFORMSIZE = "uniformsize";

  public static final String CONF_LABEL_MAX_CHUNKS_TOLERABLE = "distcp.dynamic.max.chunks.tolerable";
  public static final String CONF_LABEL_MAX_CHUNKS_IDEAL = "distcp.dynamic.max.chunks.ideal";
  public static final String CONF_LABEL_MIN_RECORDS_PER_CHUNK = "distcp.dynamic.min.records_per_chunk";
  public static final String CONF_LABEL_SPLIT_RATIO = "distcp.dynamic.split.ratio";

  /* Total bytes to be copied. Updated by copylisting. Unfiltered count */
  public static final String CONF_LABEL_TOTAL_BYTES_TO_BE_COPIED = "mapred.total.bytes.expected";

  /* Total number of paths to copy, includes directories. Unfiltered count */
  public static final String CONF_LABEL_TOTAL_NUMBER_OF_RECORDS = "mapred.number.of.records";

  /* If input is based -f <<source listing>>, file containing the src paths */
  public static final String CONF_LABEL_LISTING_FILE_PATH = "distcp.listing.file.path";

  /*
   * Directory where the final data will be committed to.
   */
  public static final String CONF_LABEL_TARGET_FINAL_PATH = "distcp.target.final.path";

  /**
   * S3MapReduceCp job id for consumers of the Disctp
   */
  public static final String CONF_LABEL_DISTCP_JOB_ID = "distcp.job.id";

  /* Meta folder where the job's intermediate data is kept */
  public static final String CONF_LABEL_META_FOLDER = "distcp.meta.folder";

  /* S3MapReduceCp CopyListing class override param */
  public static final String CONF_LABEL_COPY_LISTING_CLASS = "distcp.copy.listing.class";

  /**
   * Constants for S3MapReduceCp return code to shell / consumer of ToolRunner's run
   */
  public static final int SUCCESS = 0;
  public static final int INVALID_ARGUMENT = -1;
  public static final int DUPLICATE_INPUT = -2;
  public static final int UNKNOWN_ERROR = -999;

  /**
   * Constants for S3MapReduceCp default values of configurable values
   */
  public static final int MAX_CHUNKS_TOLERABLE_DEFAULT = 400;
  public static final int MAX_CHUNKS_IDEAL_DEFAULT = 100;
  public static final int MIN_RECORDS_PER_CHUNK_DEFAULT = 5;
  public static final int SPLIT_RATIO_DEFAULT = 2;

  /* Default number of S3 upload retries for S3MapReduceCp */
  public static final int DEFAULT_UPLOAD_RETRIES = 3;

  /* Default number of milliseconds before the next upload retry for S3MapReduceCp */
  public static final long DEFAULT_UPLOAD_RETRY_DELAY_MS = 300L;

  /* Default buffer size used during data transfer: 0 means use the default provided by the file system */
  public static final int DEFAULT_UPLOAD_BUFFER_SIZE = 0;

  private S3MapReduceCpConstants() {}
}
