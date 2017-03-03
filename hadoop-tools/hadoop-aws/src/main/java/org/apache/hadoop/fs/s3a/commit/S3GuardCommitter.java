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

package org.apache.hadoop.fs.s3a.commit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;

import java.io.IOException;

import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitUtils.*;

/**
 * This is a dedicated committer which only works with consistent
 * S3A FileSystems.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class S3GuardCommitter extends PathOutputCommitter {
  private static final Logger LOG =
      LoggerFactory.getLogger(S3GuardCommitter.class);

  /**
   * Name of this class: {@value}.
   */
  public static final String NAME
      = "org.apache.hadoop.fs.s3a.commit.S3GuardCommitterFactory";

  private Path outputPath;
  private Path workPath;
  private S3AFileSystem destFS;
  private FileCommitActions commitActions;

  public S3GuardCommitter(Path outputPath,
      JobContext context) throws IOException {
    if (outputPath != null) {
      Configuration conf = context.getConfiguration();
      destFS = getDestFS(outputPath, conf);
      commitActions = new FileCommitActions(destFS);
      this.outputPath = destFS
          .makeQualified(outputPath);
    }
    LOG.debug("Committer instantiated for job \"{}\" ID {} with destination {}",
        context.getJobName(), context.getJobID(), outputPath);
  }

  /**
   * Create a committer.
   * @param outputPath the job's output path, or null if you want the output
   * committer to act as a no-op.
   * @param context the task's context
   * @throws IOException on a failure
   */
  public S3GuardCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    this(outputPath, (JobContext) context);
    if (outputPath != null) {
      workPath = getTaskAttemptPath(context);
      destFS = getDestFS(outputPath, context.getConfiguration());
      commitActions = new FileCommitActions(destFS);
      verifyIsDelayedCommitPath(destFS, workPath);
    }
    LOG.debug("Committer instantiated for task ID {} for job \"{}\" " +
            "ID {} with work path {}",
        context.getTaskAttemptID(),
        context.getJobName(), context.getJobID(),
        workPath);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "S3AOutputCommitter{");
    sb.append("outputPath=").append(outputPath);
    sb.append(", workPath=").append(workPath);
    sb.append('}');
    return sb.toString();
  }


  /**
   * Get the destination filesystem from the output path and the configuration.
   * @param out output path
   * @param conf job/task config
   * @return the associated FS
   * @throws PathCommitException output path isn't to an S3A FS instance.
   * @throws IOException failure to instantiate the FS.
   */
  protected S3AFileSystem getDestFS(Path out, Configuration conf)
      throws IOException {
    return getS3AFS(out, conf, true);
  }

  /**
   * Get the S3A FS of a path.
   * @param path path to examine
   * @param conf config
   * @param delayedCommitRequired is delayed complete requires of the FS
   * @throws PathCommitException output path isn't to an S3A FS instance, or
   * if {code delayedCommitRequired} is set, if doesn't support delayed commits.
   * @throws IOException failure to instantiate the FS.
   */
  public static S3AFileSystem getS3AFS(Path path,
      Configuration conf,
      boolean delayedCommitRequired)
      throws IOException, PathCommitException {
    FileSystem fs = path.getFileSystem(conf);
    verifyIsS3AFS(fs, path);
    S3AFileSystem s3a = (S3AFileSystem) fs;
    if (delayedCommitRequired) {
      verifyIsDelayedCommitFS(s3a);
    }
    return s3a;
  }

  /**
   * Final path of output; this is the one the __pending dir will go
   * underneath.
   * @return the path
   */
  public Path getOutputPath() {
    return outputPath;
  }

  /**
   * Probe for an output path being defined.
   * @return true if we have an output path set, else false.
   */
  private boolean hasOutputPath() {
    return this.outputPath != null;
  }

  @Override
  public Path getWorkPath() throws IOException {
    return workPath;
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    LOG.debug("Setting up job {}", context.getJobID());
    if (hasOutputPath()) {
      try (DurationInfo d =
               new DurationInfo("Setup Job %s", context.getJobID())) {
        Path jobAttemptPath = getJobAttemptPath(context);
        S3AFileSystem fs = getDestFS(jobAttemptPath,
            context.getConfiguration());
        if (!fs.mkdirs(jobAttemptPath)) {
          throw new PathCommitException(jobAttemptPath,
              "Failed to mkdir path -it already exists");
        }
      }
    } else {
      LOG.warn("Output Path is null in setupJob()");
    }
  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    LOG.debug("Committing job {}", context.getJobID());
    if (outputPath == null) {
      LOG.warn("No output path");
      return;
    }
    // force a check for the job attempt to exist. If it
    // doesn't then either the job wasn't set up, its finished.
    // or something got in the way
    destFS.getFileStatus(getJobAttemptPath(context));

    cleanupJob(context);

    // True if the job requires output.dir marked on successful job.
    // Note that by default it is set to true.
    if (context.getConfiguration().getBoolean(
        Constants.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)) {
      commitActions.touchSuccessMarker(getOutputPath());
    }
  }

  @Override
  public void abortJob(JobContext context, JobStatus.State state)
      throws IOException {
    LOG.info("Abort Job {} in state {}", context.getJobID(), state);
    try (DurationInfo duration = new DurationInfo("Abort")) {
      if (commitActions != null) {
        Path pending = pendingSubdir(outputPath);
        FileCommitActions.CommitAllFilesOutcome outcome
            = commitActions.abortAllPendingFilesInPath(pending, true);
        outcome.maybeRethrow();
      }
    } finally {
      cleanupJob(context);
    }
  }

  @Override
  public void cleanupJob(JobContext context) throws IOException {
    LOG.info("Cleanup Job {}", context.getJobID());
    try (DurationInfo duration = new DurationInfo("Cleanup")) {
      if (hasOutputPath()) {
        deleteWithWarning(destFS, getPendingJobAttemptsPath(outputPath), true);
        deleteWithWarning(destFS,
            getTempJobAttemptPath(getAppAttemptId(context),
                getPendingJobAttemptsPath(outputPath)), true);
      }
    }
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    LOG.info("Setup Task {}", context.getTaskAttemptID());
    Path taskAttemptPath = getTaskAttemptPath(context);
    destFS.mkdirs(taskAttemptPath);
  }

  /**
   * Did this task write any files in the work directory?
   * @param context the task's context
   */
  @Override
  public boolean needsTaskCommit(TaskAttemptContext context)
      throws IOException {
    return needsTaskCommit(context, null);
  }

  /**
   * Probe for a task existing by looking to see if the attempt dir exists.
   * This adds 4 HTTP requests to the call. It may be better just to
   * return true and rely on the commit task doing the work.
   * @param context task context
   * @param taskAttemptPath path to the attempt
   * @return true if the attempt path exists
   * @throws IOException failure to list the path
   */
  private boolean needsTaskCommit(TaskAttemptContext context,
      Path taskAttemptPath) throws IOException {
    try (DurationInfo duration =
             new DurationInfo("needsTaskCommit task %s",
                 context.getTaskAttemptID())) {
      if (hasOutputPath()) {
        if (taskAttemptPath == null) {
          taskAttemptPath = getTaskAttemptPath(context);
        }
        FileSystem fs = taskAttemptPath.getFileSystem(
            context.getConfiguration());
        return fs.exists(taskAttemptPath);
      }
      return false;
    }
  }


  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    try (DurationInfo duration = new DurationInfo("Commit task %s",
        context.getTaskAttemptID())) {
      FileCommitActions.CommitAllFilesOutcome outcome =
          innerCommitTask(context);
      outcome.maybeRethrow();
    } finally {
      // delete the task attempt so there's no possibility of a second attempt
      deleteQuietly(destFS, getTaskAttemptPath(context), true);
    }
  }

  /**
   * Inner routine for committing a task.
   * @param context context
   * @return the outcome
   * @throws IOException exception
   */
  @VisibleForTesting
  FileCommitActions.CommitAllFilesOutcome innerCommitTask(
      TaskAttemptContext context) throws IOException {
    return commitActions.commitAllPendingFilesInPath(
    getTaskAttemptPath(context), true);
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    Path attemptPath = getTaskAttemptPath(context);
    try (DurationInfo duration =
             new DurationInfo("Abort task %s", context.getTaskAttemptID())) {
      FileCommitActions.CommitAllFilesOutcome outcome
          = commitActions.abortAllPendingFilesInPath(
          attemptPath, true);
      outcome.maybeRethrow();
    } finally {
      deleteQuietly(destFS, attemptPath, true);
      deleteQuietly(destFS, getTempTaskAttemptPath(context), true);
    }
  }

  @Override
  public boolean isRecoverySupported() {
    return false;
  }

  @Override
  public boolean isCommitJobRepeatable(JobContext jobContext)
      throws IOException {
    return false;
  }

  @Override
  public void recoverTask(TaskAttemptContext taskContext) throws IOException {
    IOException unsupported = new IOException("Unsupported");
    LOG.warn("Unable to recover task {}", taskContext.getTaskAttemptID(),
        unsupported);
    throw unsupported;
  }

  /**
   * Compute the path where the output of a given job attempt will be placed.
   * @param context the context of the job.  This is used to get the
   * application attempt id.
   * @return the path to store job attempt data.
   */
  public Path getJobAttemptPath(JobContext context) {
    return getJobAttemptPath(getAppAttemptId(context));
  }


  /**
   * Compute the path where the output of a given job attempt will be placed.
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path to store job attempt data.
   */
  protected Path getJobAttemptPath(int appAttemptId) {
    return CommitUtils.getJobAttemptPath(appAttemptId, getOutputPath());
  }

  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   *
   * @param context the context of the task attempt.
   * @return the path where a task attempt should be stored.
   */
  public Path getTaskAttemptPath(TaskAttemptContext context) {
    return CommitUtils.getTaskAttemptPath(context, getOutputPath());
  }

  /**
   * Get a temporary directory for data. When a task is aborted/cleaned
   * up, the contents of this directory are all deleted.
   * @param context task context
   * @return a path for temporary data.
   */
  public Path getTempTaskAttemptPath(TaskAttemptContext context) {
    return CommitUtils.getTempTaskAttemptPath(context, getOutputPath());
  }

  /**
   * A duration with logging of final state at info in the close() call.
   */
  private static final class DurationInfo extends Duration
      implements AutoCloseable {
    private final String text;

    private DurationInfo(String format, Object... args) {
      this.text = String.format(format, args);
      LOG.info("Starting {}", text);
    }

    @Override
    public String toString() {
      return text + ": " + super.toString();
    }

    @Override
    public void close() {
      finished();
      LOG.info("{}", this);
    }
  }
}
