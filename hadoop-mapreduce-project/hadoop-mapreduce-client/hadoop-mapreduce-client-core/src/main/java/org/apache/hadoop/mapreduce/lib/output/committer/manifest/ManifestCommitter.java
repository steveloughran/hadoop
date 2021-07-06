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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;

import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.logIOStatisticsAtDebug;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.CleanupJobStage.cleanupStageOptionsFromConfig;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.MANIFEST_SUFFIX;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_SUMMARY_REPORT_DIR;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASKS_COMPLETED;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_ABORT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterSupport.createIOStatisticsStore;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterSupport.createManifestOutcome;

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
  private ManifestSuccessData successData;

  /**
   * The active stage; is updated by a callback from within the stages.
   */
  private String activeStage;

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
    baseConfig = createCommitterConfig(true, context);
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
        iostatistics,
        this::enterStage);
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
   * Get the manifest success data for this job; creating on demand if needed.
   * @param committerConfig source config.
   * @return the current {@link #successData} value; never null.
   */
  private ManifestSuccessData maybeCreateJobSuccessData(
      ManifestCommitterConfig committerConfig) {
    if (successData == null) {
      successData = createManifestOutcome(
          committerConfig.createJobStageConfig());
    }
    return successData;
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

    maybeCreateJobSuccessData(committerConfig);
    try (CloseableTaskSubmitter ioProcs =
             committerConfig.createSubmitter();
         StoreOperations storeOperations = createStoreOperations()) {
      // the stage config will be shared across all stages.
      StageConfig stageConfig = committerConfig.createJobStageConfig()
          .withOperations(storeOperations)
          .withIOProcessors(ioProcs)
          .build();

      // commit the the manifests
      CommitJobStage.Result result = new CommitJobStage(stageConfig)
          .apply(committerConfig.getCreateJobMarker());
      successData = result.getJobSuccessData();

      // clean up job attempt dir if not disabled
      // note: it's a no-op if the options don't say "enabled"
      new CleanupJobStage(stageConfig).apply(
          cleanupStageOptionsFromConfig(
              jobContext.getConfiguration()));

      // and then, after everything else: validate.

      if (committerConfig.getValidateOutput()) {
        LOG.info("Validating output.");
        new ValidateRenamedFilesStage(stageConfig)
            .apply(result.getManifests());
      }
    } catch (IOException e){
      // failure.
      successData.jobFailure(e);
      throw e;
    } finally {
      maybeSaveSummary(committerConfig,
          successData,
          true);
      // print job commit stats
      LOG.info("Job Commit statistics {}",
          ioStatisticsToPrettyString(iostatistics));
    }
  }

  /**
   * Abort the job.
   * Invokes {@link #cleanupJob(JobContext)} operation then
   * saves the (ongoing) job success data if reporting is enabled.
   * @param jobContext Context of the job whose output is being written.
   * @param state final runstate of the job
   * @throws IOException
   */
  @Override
  public void abortJob(final JobContext jobContext,
      final JobStatus.State state)
      throws IOException {
    ManifestCommitterConfig committerConfig = createCommitterConfig(false,
        jobContext);
    maybeCreateJobSuccessData(committerConfig);

    try {
      cleanupJob(jobContext);
    } catch (IOException e) {
      // failure.
      successData.jobFailure(e);
    } finally {
      successData.setSuccess(false);
      successData.setStage(OP_STAGE_JOB_ABORT);
      maybeSaveSummary(committerConfig,
          successData,
          true);
    }
    // print job stats
    LOG.info("Job Abort statistics {}",
        ioStatisticsToPrettyString(iostatistics));
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

  /**
   * Callback on stage entry.
   * @param stage new stage
   */
  public void enterStage(String stage) {
    activeStage = stage;
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
  ManifestSuccessData getSuccessData() {
    return successData;
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
   * Save a summary to the report dir if the config option
   * is set.
   * @param operations store operations to use.
   * @param config configuration to use.
   * @param summary summary file.
   * @param quiet should exceptions be swallowed.
   * @return the path of a file, if successfully saved
   * @throws IOException if a failure occured and quiet==false
   */
  private Path maybeSaveSummary(
      ManifestCommitterConfig config,
      ManifestSuccessData summary,
      boolean quiet) throws IOException {
    Configuration conf = config.getConf();
    String reportDir = conf.getTrimmed(OPT_SUMMARY_REPORT_DIR, "");
    if (reportDir.isEmpty()) {
      return null;
    }
    Path reportDirPath = new Path(reportDir);
    Path path = new Path(reportDirPath,
        String.format("summary-%s.json",
            config.getJobUniqueId()));
    try (StoreOperations operations =
             new StoreOperationsThroughFileSystem(path.getFileSystem(conf))) {
      operations.save(summary, path, true);
      LOG.info("Job summary saved to {}", path);
      return path;
    } catch (IOException e) {
      LOG.debug("Failed to save summary to {}", path, e);
      if (quiet) {
        return null;
      } else {
        throw e;
      }
    }
  }
}
