/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.DistCp} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/DistCp.java
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

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.circustrain.s3mapreducecp.CopyListing.DuplicateFileException;
import com.hotels.bdp.circustrain.s3mapreducecp.CopyListing.InvalidInputException;
import com.hotels.bdp.circustrain.s3mapreducecp.aws.AwsS3ClientFactory;
import com.hotels.bdp.circustrain.s3mapreducecp.mapreduce.CopyMapper;
import com.hotels.bdp.circustrain.s3mapreducecp.mapreduce.CopyOutputFormat;
import com.hotels.bdp.circustrain.s3mapreducecp.util.ConfigurationUtil;
import com.hotels.bdp.circustrain.s3mapreducecp.util.PathUtil;

/**
 * S3MapReduceCp is the main driver-class. For command-line use S3MapReduceCp::main() orchestrates the parsing of
 * command-line parameters and the launch of the S3MapReduceCp job. For programmatic use a S3MapReduceCp object can be
 * constructed by specifying options (in a S3MapReduceCpOptions object) and S3MapReduceCp::execute() may be used to
 * launch the copy-job. S3MapReduceCp may alternatively be sub-classed to fine-tune behaviour.
 */
public class S3MapReduceCp extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(S3MapReduceCp.class);

  /**
   * Priority of the ResourceManager shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private S3MapReduceCpOptions inputOptions;
  private Path metaFolder;

  private static final String PREFIX = "_s3mapreducecp";
  private static final String S3MAPREDUCECP_DEFAULT_XML = "s3mapreducecp-default.xml";
  public static final Random rand = new Random();

  private final AwsS3ClientFactory awsS3ClientFactory = new AwsS3ClientFactory();

  private boolean submitted;
  private FileSystem jobFS;

  /**
   * Public Constructor. Creates S3MapReduceCp object with specified input-parameters. (E.g. source-paths,
   * target-location, etc.)
   *
   * @param inputOptions Options (indicating source-paths, target-location.)
   * @param configuration The Hadoop configuration against which the Copy-mapper must run.
   * @throws Exception on failure.
   */
  public S3MapReduceCp(Configuration configuration, S3MapReduceCpOptions inputOptions) throws Exception {
    Configuration config = new S3MapReduceCpConfiguration(configuration);
    config.addResource(S3MAPREDUCECP_DEFAULT_XML);
    setConf(config);
    this.inputOptions = inputOptions;
    metaFolder = createMetaFolderPath();
  }

  /**
   * To be used with the ToolRunner. Not for public consumption.
   */
  @VisibleForTesting
  public S3MapReduceCp() {}

  /**
   * Implementation of Tool::run(). Orchestrates the copy of source file(s) to target location, by: 1. Creating a list
   * of files to be copied to target. 2. Launching a Map-only job to copy the files. (Delegates to execute().)
   *
   * @param argv List of arguments passed to S3MapReduceCp, from the ToolRunner.
   * @return On success, it returns 0. Else, -1.
   */
  @Override
  public int run(String[] argv) {
    OptionsParser optionsParser = new OptionsParser();
    inputOptions = optionsParser.parse(argv);
    LOG.info("Input Options: {}", inputOptions);

    try {
      execute();
    } catch (InvalidInputException e) {
      LOG.error("Invalid input: ", e);
      return S3MapReduceCpConstants.INVALID_ARGUMENT;
    } catch (DuplicateFileException e) {
      LOG.error("Duplicate files in input path: ", e);
      return S3MapReduceCpConstants.DUPLICATE_INPUT;
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      return S3MapReduceCpConstants.UNKNOWN_ERROR;
    }
    return S3MapReduceCpConstants.SUCCESS;
  }

  /**
   * Implements the core-execution. Creates the file-list for copy and launches the Hadoop-job to do the copy.
   *
   * @return Job handle
   * @throws Exception if something goes wrong.
   */
  public Job execute() throws Exception {
    assert inputOptions != null;
    assert getConf() != null;

    Job job = null;
    try {
      synchronized (this) {
        // Don't cleanup while we are setting up.
        metaFolder = createMetaFolderPath();
        jobFS = metaFolder.getFileSystem(getConf());

        prepareConf();

        job = createJob();
      }
      createInputFileListing(job);

      job.submit();
      submitted = true;
    } finally {
      if (!submitted) {
        cleanup();
      }
    }

    String jobID = job.getJobID().toString();
    job.getConfiguration().set(S3MapReduceCpConstants.CONF_LABEL_DISTCP_JOB_ID, jobID);

    LOG.info("S3MapReduceCp job-id: {}", jobID);
    if (inputOptions.isBlocking() && !job.waitForCompletion(true)) {
      throw new IOException("S3MapReduceCp failure: Job " + jobID + " has failed: " + job.getStatus().getFailureInfo());
    }
    return job;
  }

  private void prepareConf() throws IOException {
    // We call this method because the code below tries to read some properties from the Hadoop Configuration object.
    overwriteConf();

    Path logPath = inputOptions.getLogPath();
    if (logPath == null) {
      logPath = new Path(metaFolder, "_logs");
    } else {
      LOG.info("S3MapReduceCp job log path: {}", logPath);
    }
    inputOptions.setLogPath(logPath);

    String region = inputOptions.getRegion();
    if (region == null) {
      String bucketName = PathUtil.toBucketName(inputOptions.getTarget());
      try {
        AmazonS3 s3Client = awsS3ClientFactory.newInstance(getConf());
        region = s3Client.getBucketLocation(bucketName);
        inputOptions.setRegion(region);
      } catch (Exception e) {
        throw new IOException("Unable to determine region for bucket " + bucketName, e);
      }
    }

    // We finally push all the changes into the Hadoop Configuration
    overwriteConf();
  }

  private void overwriteConf() {
    Configuration conf = getConf();
    copyOptions(inputOptions, conf);
    setConf(conf);
  }

  /**
   * Sets configuration properties using the given set of options.
   *
   * @param options Options
   * @param conf Configuration
   */
  private static void copyOptions(S3MapReduceCpOptions options, Configuration conf) {
    Map<String, String> optionsMap = options.toMap();
    for (Map.Entry<String, String> option : optionsMap.entrySet()) {
      conf.set(option.getKey(), option.getValue());
    }
  }

  /**
   * Create Job object for submitting it with all the configuration
   *
   * @return Reference to job object.
   * @throws IOException Exception if any
   */
  private Job createJob() throws IOException {
    String jobName = "s3mapreducecp";
    String userChosenName = getConf().get(MRJobConfig.JOB_NAME);
    if (userChosenName != null) {
      jobName += ": " + userChosenName;
    }

    Job job = Job.getInstance(getConf());
    job.setJobName(jobName);
    job.setInputFormatClass(ConfigurationUtil.getStrategy(getConf(), inputOptions));
    job.setJarByClass(CopyMapper.class);
    configureOutputFormat(job);

    job.setMapperClass(CopyMapper.class);
    job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputFormatClass(CopyOutputFormat.class);
    job.getConfiguration().set(MRJobConfig.MAP_SPECULATIVE, "false");
    job.getConfiguration().set(MRJobConfig.NUM_MAPS, String.valueOf(inputOptions.getMaxMaps()));

    return job;
  }

  /**
   * Setup output format appropriately
   *
   * @param job Job handle
   * @throws IOException Exception if any
   */
  private void configureOutputFormat(Job job) throws IOException {
    CopyOutputFormat.setCommitDirectory(job, toCommitDirectory(inputOptions.getTarget()));
    FileOutputFormat.setOutputPath(job, inputOptions.getLogPath());
  }

  private Path toCommitDirectory(URI target) {
    Path path = new Path(target);
    if (PathUtil.isFile(target)) {
      return path.getParent();
    }
    return path;
  }

  /**
   * Create input listing by invoking an appropriate copy listing implementation. Also add delegation tokens for each
   * path to job's credential store
   *
   * @param job Handle to job
   * @return Returns the path where the copy listing is created
   * @throws IOException If any
   */
  protected Path createInputFileListing(Job job) throws IOException {
    Path fileListingPath = getFileListingPath();
    CopyListing copyListing = CopyListing.getCopyListing(job.getConfiguration(), job.getCredentials(), inputOptions);
    copyListing.buildListing(fileListingPath, inputOptions);
    return fileListingPath;
  }

  /**
   * Get default name of the copy listing file. Use the meta folder to create the copy listing file
   *
   * @return - Path where the copy listing file has to be saved
   * @throws IOException - Exception if any
   */
  protected Path getFileListingPath() throws IOException {
    String fileListPathStr = metaFolder + "/fileList.seq";
    Path path = new Path(fileListPathStr);
    return new Path(path.toUri().normalize().toString());
  }

  /**
   * Create a default working folder for the job, under the job staging directory
   *
   * @return Returns the working folder information
   * @throws Exception - EXception if any
   */
  private Path createMetaFolderPath() throws Exception {
    Configuration configuration = getConf();
    Path stagingDir = JobSubmissionFiles.getStagingDir(new Cluster(configuration), configuration);
    Path metaFolderPath = new Path(stagingDir, PREFIX + String.valueOf(rand.nextInt()));
    LOG.debug("Meta folder location: {}", metaFolderPath);
    configuration.set(S3MapReduceCpConstants.CONF_LABEL_META_FOLDER, metaFolderPath.toString());
    return metaFolderPath;
  }

  /**
   * Main function of the S3MapReduceCp program. Parses the input arguments (via OptionsParser), and invokes the
   * S3MapReduceCp::run() method, via the ToolRunner.
   *
   * @param args Command-line arguments sent to S3MapReduceCp.
   */
  public static void main(String[] args) {
    OptionsParser optionsParser = new OptionsParser();
    if (args.length < 1) {
      optionsParser.usage();
      System.exit(S3MapReduceCpConstants.INVALID_ARGUMENT);
    }

    S3MapReduceCpOptions options = null;
    try {
      options = optionsParser.parse(args);
      if (options.isHelp()) {
        System.exit(S3MapReduceCpConstants.SUCCESS);
      }
    } catch (Throwable e) {
      LOG.error("Invalid arguments: ", e);
      optionsParser.usage();
      System.exit(S3MapReduceCpConstants.INVALID_ARGUMENT);
    }

    int exitCode;
    try {
      S3MapReduceCp s3MapReduceCp = new S3MapReduceCp();
      Cleanup cleanUp = new Cleanup(s3MapReduceCp);

      ShutdownHookManager.get().addShutdownHook(cleanUp, SHUTDOWN_HOOK_PRIORITY);
      exitCode = ToolRunner.run(getDefaultConf(options), s3MapReduceCp, args);
    } catch (Exception e) {
      LOG.error("Couldn't complete S3MapReduceCp operation: ", e);
      exitCode = S3MapReduceCpConstants.UNKNOWN_ERROR;
    }
    System.exit(exitCode);
  }

  /**
   * Loads properties from s3mapreducecp-default.xml into configuration object
   *
   * @return Configuration which includes properties from s3mapreducecp-default.xml
   */
  private static Configuration getDefaultConf(S3MapReduceCpOptions options) {
    Configuration config = new S3MapReduceCpConfiguration();
    config.addResource(S3MAPREDUCECP_DEFAULT_XML);
    if (options.getCredentialsProvider() != null) {
      config.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, options.getCredentialsProvider().toString());
    }
    return config;
  }

  private synchronized void cleanup() {
    try {
      if (metaFolder == null) {
        return;
      }

      jobFS.delete(metaFolder, true);
      metaFolder = null;
    } catch (IOException e) {
      LOG.error("Unable to cleanup meta folder: {}", metaFolder, e);
    }
  }

  private boolean isSubmitted() {
    return submitted;
  }

  private static class Cleanup implements Runnable {
    private final S3MapReduceCp s3MapReduceCp;

    public Cleanup(S3MapReduceCp s3MapReduceCp) {
      this.s3MapReduceCp = s3MapReduceCp;
    }

    @Override
    public void run() {
      if (s3MapReduceCp.isSubmitted()) {
        return;
      }

      s3MapReduceCp.cleanup();
    }
  }
}
