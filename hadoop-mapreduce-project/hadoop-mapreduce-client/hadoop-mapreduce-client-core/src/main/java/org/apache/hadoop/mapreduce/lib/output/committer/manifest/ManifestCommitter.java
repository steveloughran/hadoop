/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import java.io.IOException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.logIOStatisticsAtDebug;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.CleanupJobStage.cleanupStageOptionsFromConfig;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.DEFAULT_CREATE_SUCCESSFUL_JOB_DIR_MARKER;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.MANIFEST_SUFFIX;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_IO_PROCESSORS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_IO_PROCESSORS_DEFAULT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_VALIDATE_OUTPUT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_VALIDATE_OUTPUT_DEFAULT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASKS_COMPLETED;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterSupport.buildJobUUID;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterSupport.createIOStatisticsStore;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterSupport.getAppAttemptId;

/**
 * This is the Intermediate-Manifest committer.
 */
public class ManifestCommitter extends PathOutputCommitter implements
    IOStatisticsSource {

  public static final Logger LOG = LoggerFactory.getLogger(
      ManifestCommitter.class);

  public static final String TASK_COMMITTER = "task committer";

  public static final String JOB_COMMITTER = "job committer";

  /**
   * Committer Configuration as extracted from
   * the job/task context and set in the constructor.
   *
   */
  private final ManifestCommitterConfig baseConfig;

  /**
   * Destination of the job.
   */
  private final Path destinationDir;

  /**
   * For tasks, the attempt directory.
   * Null for jobs.
   */
  private final Path taskAttemptDir;

  /**
   * IOStatistics to update.
   */
  private final IOStatisticsStore iostatistics;

  /**
   *  The job Manifest Success data; only valid after a job successfully
   *  commits.
   */
  private ManifestSuccessData jobSuccessData;

  /**
   * The task manifest of the task commit.
   * Null unless this is a task attempt and the
   * task has successfully been committed.
   */
  private TaskManifest taskAttemptCommittedManifest;

  /**
   * Create a committer.
   * @param outputPath output path
   * @param context job/task context
   * @throws IOException failure.
   */
  ManifestCommitter(final Path outputPath,
      final TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    destinationDir = resolveDestinationDirectory(outputPath,
        context.getConfiguration());
    iostatistics = createIOStatisticsStore().build();
    baseConfig = new ManifestCommitterConfig(destinationDir,
        TASK_COMMITTER,
        context,
        iostatistics);
    taskAttemptDir = baseConfig.getTaskAttemptDir();
  }

  /**
   * Create a committer config from the passed in job/task context.
   * @param isTask is this a task?
   * @param context context
   * @return committer config
   */
  private ManifestCommitterConfig createCommitterConfig(boolean isTask,
      JobContext context) {
    return new ManifestCommitterConfig(
        getOutputPath(),
        isTask ? TASK_COMMITTER : JOB_COMMITTER,
        context,
        iostatistics);
  }

  /**
   * Set up a job through a {@link SetupJobStage}.
   * @param jobContext Context of the job whose output is being written.
   * @throws IOException IO Failure.
   */
  public void setupJob(final JobContext jobContext) throws IOException {
    ManifestCommitterConfig committerConfig = createCommitterConfig(false,
        jobContext);
    StageConfig stageConfig =
        committerConfig
            .createJobStageConfig()
            .withOperations(createStoreOperations())
            .build();
    // set up the job.
    new SetupJobStage(stageConfig)
        .apply(committerConfig.getCreateJobMarker());
    logCommitterStatisticsAtDebug();
  }

  /**
   * Set up a task through a {@link SetupTaskStage}.
   *
   * @param context task context.
   * @throws IOException IO Failure.
   */
  public void setupTask(final TaskAttemptContext context)
      throws IOException {
    StageConfig stageConfig =
        createCommitterConfig(true, context)
            .createJobStageConfig()
            .withOperations(createStoreOperations())
            .build();
    // create task attempt dir; delete if present. Or fail?
    new SetupTaskStage(stageConfig).apply("");
    logCommitterStatisticsAtDebug();
  }

  /**
   * Always return true.
   * @param context task context.
   * @return true
   * @throws IOException IO Failure.
   */
  public boolean needsTaskCommit(final TaskAttemptContext context)
      throws IOException {
    return true;
  }

  /**
   * Failure during Job Commit is not recoverable from.
   *
   * @param jobContext
   *          Context of the job whose output is being written.
   * @return false, always
   * @throws IOException never
   */
  @Override
  public boolean isCommitJobRepeatable(final JobContext jobContext)
      throws IOException {
    return false;
  }

  /**
   * Declare that task recovery is not supported.
   * It would be, if someone added the code *and tests*.
   * @param jobContext
   *          Context of the job whose output is being written.
   * @return false, always
   * @throws IOException never
   */
  @Override
  public boolean isRecoverySupported(final JobContext jobContext)
      throws IOException {
    return false;
  }

  /**
   *
   * @param taskContext Context of the task whose output is being recovered
   * @throws IOException always
   */
  @Override
  public void recoverTask(final TaskAttemptContext taskContext)
      throws IOException {
    LOG.warn("Rejecting recoverTask({}) call", taskContext.getTaskAttemptID());
    throw new IOException("Cannot recover task "
        + taskContext.getTaskAttemptID());
  }

  /**
   * Commit the task.
   * This is where the task attempt tree list takes place.
   * @param context task context.
   * @throws IOException IO Failure.
   */
  public void commitTask(final TaskAttemptContext context)
      throws IOException {
    ManifestCommitterConfig committerConfig = createCommitterConfig(true,
        context);
    StageConfig stageConfig = committerConfig.createJobStageConfig()
        .withOperations(createStoreOperations())
        .build();
    taskAttemptCommittedManifest = new CommitTaskStage(stageConfig)
        .apply("generate")
        .getRight();
    iostatistics.incrementCounter(COMMITTER_TASKS_COMPLETED, 1);
    logCommitterStatisticsAtDebug();
  }

  /**
   * Abort a task.
   * @param context task context
   * @throws IOException failure during the delete
   */
  public void abortTask(final TaskAttemptContext context)
      throws IOException {
    ManifestCommitterConfig committerConfig = createCommitterConfig(true,
        context);
    new AbortTaskStage(committerConfig.createJobStageConfig()
        .withOperations(createStoreOperations())
        .build())
        .apply(false);
    logCommitterStatisticsAtDebug();
  }

  /**
   * This is the big job commit stage.
   * Load the manifests, prepare the destination, rename
   * the files then cleanup the job directory.
   * @param jobContext Context of the job whose output is being written.
   * @throws IOException failure.
   */
  @Override
  public void commitJob(final JobContext jobContext) throws IOException {

    ManifestCommitterConfig committerConfig = createCommitterConfig(false,
        jobContext);

    try (CloseableTaskSubmitter ioProcs =
             committerConfig.createSubmitter()) {
      // the stage config will be shared across all stages.
      StageConfig stageConfig = committerConfig.createJobStageConfig()
          .withOperations(createStoreOperations())
          .withIOProcessors(ioProcs)
          .build();

      // load the manifests
      jobSuccessData = new CommitJobStage(stageConfig)
          .apply(Pair.of(committerConfig.getCreateJobMarker(),
              committerConfig.getValidateOutput()));

      // clean up job attempt dir if not disabled
      // note: it's a no-op if the options don't say "enabled"
      new CleanupJobStage(stageConfig).apply(
          cleanupStageOptionsFromConfig(
              jobContext.getConfiguration()));
    } finally {
      // print job commit stats
      LOG.info("Job Commit statistics {}",
          ioStatisticsToPrettyString(iostatistics));
    }
  }

  @Override
  public void abortJob(final JobContext jobContext,
      final JobStatus.State state)
      throws IOException {
    cleanupJob(jobContext);
  }

  /**
   * Execute the {@code CleanupJobStage} to remove the job attempt dir.
   * This does
   * @param jobContext Context of the job whose output is being written.
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  @Override
  public void cleanupJob(final JobContext jobContext) throws IOException {
    ManifestCommitterConfig committerConfig = createCommitterConfig(false,
        jobContext);
    try (CloseableTaskSubmitter ioProcs =
             committerConfig.createSubmitter()) {

      new CleanupJobStage(committerConfig
          .createJobStageConfig()
          .withOperations(createStoreOperations())
          .withIOProcessors(ioProcs).build())
          .apply(cleanupStageOptionsFromConfig(
              jobContext.getConfiguration()));
    }
    logCommitterStatisticsAtDebug();
  }

  /**
   * Output path: destination directory of the job.
   * @return the overall job destination directory.
   */
  public Path getOutputPath() {
    return getDestinationDir();
  }

  /**
   * Work path of the current task attempt.
   * This is null if the task does not have one.
   * @return a path.
   */
  public Path getWorkPath() {
    return getTaskAttemptDir();
  }

  private Path getDestinationDir() {
    return destinationDir;
  }

  private Path getTaskAttemptDir() {
    return taskAttemptDir;
  }

  String getJobUniqueId() {
    return baseConfig.getJobUniqueId();
  }

  Configuration getConf() {
    return baseConfig.getConf();
  }

  /**
   * Get the manifest Success data; only valid after a job.
   * @return the job _SUCCESS data, or null.
   */
  ManifestSuccessData getJobSuccessData() {
    return jobSuccessData;
  }

  /**
   * Get the manifest of the last committed task.
   * @return a task manifest or null.
   */
  TaskManifest getTaskAttemptCommittedManifest() {
    return taskAttemptCommittedManifest;
  }

  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   * @param context the context of the task attempt.
   * @return the path where a task attempt should be stored.
   */
  public Path getTaskAttemptPath(TaskAttemptContext context) {
    return createCommitterConfig(false, context).getTaskAttemptDir();
  }

  /**
   * The path to where the manifest file of a task attempt will be
   * saved when the task is committed.
   * This path will be the same for all attempts of the same task.
   * @param context the context of the task attempt.
   * @return the path where a task attempt should be stored.
   */
  public Path getTaskManifestPath(TaskAttemptContext context) {
    final Path dir = createCommitterConfig(false,
        context).getJobAttemptDir();
    Path manifestFile = new Path(dir,
        context.getTaskAttemptID().getTaskID().toString() + MANIFEST_SUFFIX);

    return manifestFile;
  }

  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   * @param context the context of the task attempt.
   * @return the path where a task attempt should be stored.
   */
  public Path getJobAttemptPath(JobContext context) {

    return createCommitterConfig(false, context).getJobAttemptDir();
  }

  /**
   * Get the final output path, including resolving any relative path.
   * @param outputPath output path
   * @param conf configuration to create any FS with
   * @return a resolved path.
   * @throws IOException failure.
   */
  private Path resolveDestinationDirectory(Path outputPath,
      Configuration conf) throws IOException {
    return FileSystem.get(outputPath.toUri(), conf).makeQualified(outputPath);
  }

  /**
   * Create a FS level store operations.
   * This is a point which can be overridden during testing.
   * @return a new store operations instance bonded to the destination fs.
   * @throws IOException failure to instantiate.
   */
  protected StoreOperations createStoreOperations() throws IOException {
    return new StoreOperationsThroughFileSystem(
        baseConfig.getDestinationFileSystem());
  }

  /**
   * Log IO Statistics at debug.
   */
  private void logCommitterStatisticsAtDebug() {
    logIOStatisticsAtDebug(LOG, "Committer Statistics", this);
  }

  /**
   * The configuration for the committer as built up from the job configuration
   * and data passed down from the committer factory.
   * Isolated for ease of dev/test
   */
  static final class ManifestCommitterConfig {

    /**
     * Final destination of work.
     * This is <i>unqualified</i>.
     */
    private final Path destinationDir;

    /**
     * Role: used in log/text messages.
     */
    private final String role;

    /**
     * This is the directory for all intermediate work: where the output
     * format will write data.
     */
    private final Path taskAttemptDir;

    /** Configuration of the job. */
    private final Configuration conf;

    /** The job context. For a task, this can be cast to a TaskContext. */
    private final JobContext jobContext;

    /** Should a job marker be created? */
    private final boolean createJobMarker;

    /**
     * Job ID Or UUID -without any attempt suffix.
     * This is expected/required to be unique, though
     * Spark has had "issues" there until recently
     * with lack of uniqueness of generated MR Job IDs.
     */
    private final String jobUniqueId;

    /**
     * Where did the job Unique ID come from?
     */
    private final String jobUniqueIdSource;

    /**
     * Number of this attempt; starts at zero.
     */
    private final int jobAttemptNumber;

    /**
     * Job ID + AttemptID.
     */
    private final String jobAttemptId;

    /**
     * Task ID: used as the filename of the manifest.
     */
    private final String taskId;

    /**
     * Task attempt ID. Determines the working
     * directory for task attempts to write data into,
     * and for the task committer to scan.
     */
    private final String taskAttemptId;

    /** Any progressable for progress callbacks. */
    private final Progressable progressable;

    /**
     * IOStatistics to update.
     */
    private final IOStatisticsStore iostatistics;

    /** Should the output be validated after the commit? */
    private final boolean validateOutput;

    private final ManifestCommitterSupport.AttemptDirectories dirs;

    /**
     * Constructor.
     * @param outputPath destination path of the job.
     * @param role role for log messages.
     * @param context job/task context
     * @param iostatistics IO Statistics
     */
    ManifestCommitterConfig(
        final Path outputPath,
        final String role,
        final JobContext context,
        final IOStatisticsStore iostatistics) {
      this.role = role;
      this.jobContext = context;
      this.conf = context.getConfiguration();
      this.destinationDir = outputPath;
      this.iostatistics = iostatistics;
      this.createJobMarker = conf.getBoolean(
          SUCCESSFUL_JOB_OUTPUT_DIR_MARKER,
          DEFAULT_CREATE_SUCCESSFUL_JOB_DIR_MARKER);
      this.validateOutput = conf.getBoolean(
          OPT_VALIDATE_OUTPUT,
          OPT_VALIDATE_OUTPUT_DEFAULT);
      Pair<String, String> pair = buildJobUUID(conf, context.getJobID());
      this.jobUniqueId = pair.getLeft();
      this.jobUniqueIdSource = pair.getRight();
      this.jobAttemptNumber = getAppAttemptId(context);
      this.jobAttemptId = this.jobUniqueId + "_" + jobAttemptNumber;

      // build directories
      this.dirs = new ManifestCommitterSupport.AttemptDirectories(outputPath,
          this.jobUniqueId, jobAttemptNumber);

      // if constructed with a task attempt, build the task ID and path.
      if (context instanceof TaskAttemptContext) {
        // it's a task
        final TaskAttemptContext tac = (TaskAttemptContext) context;
        TaskAttemptID taskAttempt = Objects.requireNonNull(
            tac.getTaskAttemptID());
        taskAttemptId = taskAttempt.toString();
        taskId = taskAttempt.getTaskID().toString();
        // Task attempt dir; must be different across instances
        taskAttemptDir = dirs.getTaskAttemptPath(taskAttemptId);
        // the context is also the progress callback.
        progressable = tac;

      } else {
        // it's a job
        taskId = "";
        taskAttemptId = "";
        taskAttemptDir = null;
        progressable = null;
      }
    }

    /**
     * Get the destination filesystem.
     * @return destination FS.
     * @throws IOException Problems binding to the destination FS.
     */
    FileSystem getDestinationFileSystem() throws IOException {
      return FileSystem.get(destinationDir.toUri(), conf);
    }

    /**
     * Create the job stage config from the committer
     * configuration.
     * This does not bind the store operations
     * or processors.
     * @return a stage config with configuration options passed in.
     */
    StageConfig createJobStageConfig() {
      StageConfig stageConfig = new StageConfig();
      stageConfig
          .withIOStatistics(iostatistics)
          .withJobDirectories(dirs)
          .withJobId(jobUniqueId)
          .withJobIdSource(jobUniqueIdSource)
          .withJobAttemptNumber(jobAttemptNumber)
          .withTaskAttemptDir(taskAttemptDir)
          .withTaskAttemptId(taskAttemptId)
          .withTaskId(taskId)
          .withProgressable(progressable);

      return stageConfig;
    }

    Path getDestinationDir() {
      return destinationDir;
    }

    String getRole() {
      return role;
    }

    Path getTaskAttemptDir() {
      return taskAttemptDir;
    }

    Path getJobAttemptDir() {
      return dirs.getJobAttemptDir();
    }

    Configuration getConf() {
      return conf;
    }

    JobContext getJobContext() {
      return jobContext;
    }

    boolean getCreateJobMarker() {
      return createJobMarker;
    }

    String getJobAttemptId() {
      return jobAttemptId;
    }

    String getTaskAttemptId() {
      return taskAttemptId;
    }

    String getTaskId() {
      return taskId;
    }

    String getJobUniqueId() {
      return jobUniqueId;
    }

    boolean getValidateOutput() {
      return validateOutput;
    }

    /**
     * Create a new thread pool from the
     * {@link ManifestCommitterConstants#OPT_IO_PROCESSORS}
     * settings.
     * @return a new thread pool.
     */
    CloseableTaskSubmitter createSubmitter() {
      return createSubmitter(
          OPT_IO_PROCESSORS, OPT_IO_PROCESSORS_DEFAULT);
    }

    /**
     * Create a new thread pool.
     * This must be shut down.
     * @param key config key with pool size.
     * @param defVal default value.
     * @return a new thread pool.
     */
    CloseableTaskSubmitter createSubmitter(String key, int defVal) {
      int numThreads = conf.getInt(key, defVal);
      if (numThreads <= 0) {
        // ignore the setting if it is too invalid.
        numThreads = defVal;
      }
      return createCloseableTaskSubmitter(numThreads, getJobAttemptId());
    }

    @VisibleForTesting
    static CloseableTaskSubmitter createCloseableTaskSubmitter(
        final int numThreads,
        final String jobAttemptId) {
      return new CloseableTaskSubmitter(
          HadoopExecutors.newFixedThreadPool(numThreads,
              new ThreadFactoryBuilder()
                  .setDaemon(true)
                  .setNameFormat("manifest-committer-" + jobAttemptId + "-%d")
                  .build()));
    }

  }

}
