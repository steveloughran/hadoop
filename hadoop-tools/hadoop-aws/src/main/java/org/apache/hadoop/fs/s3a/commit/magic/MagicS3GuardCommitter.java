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

package org.apache.hadoop.fs.s3a.commit.magic;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.AbstractS3GuardCommitter;
import org.apache.hadoop.fs.s3a.commit.CommitUtils;
import org.apache.hadoop.fs.s3a.commit.DurationInfo;
import org.apache.hadoop.fs.s3a.commit.FileCommitActions;
import org.apache.hadoop.fs.s3a.commit.PathCommitException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitUtils.*;

/**
 * This is a dedicated committer which only works with consistent
 * S3A FileSystems.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MagicS3GuardCommitter extends AbstractS3GuardCommitter {
  private static final Logger LOG =
      LoggerFactory.getLogger(MagicS3GuardCommitter.class);

  /**
   * Name of this class: {@value}.
   */
  public static final String NAME
      = "org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter";

  /**
   * Instantiate.
   * @param outputPath output path
   * @param context job context
   * @throws IOException on a failure
   */
  public MagicS3GuardCommitter(Path outputPath,
      JobContext context) throws IOException {
    super(outputPath, context);
  }

  /**
   * Create a committer.
   * @param outputPath the job's output path
   * @param context the task's context
   * @throws IOException on a failure
   */
  public MagicS3GuardCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    setWorkPath(getTaskAttemptPath(context));
    verifyIsDelayedCommitPath(getDestS3AFS(), getWorkPath());
    LOG.debug("Task attempt {} has work path {}",
        context.getTaskAttemptID(),
        getWorkPath());
  }

  @Override
  public String getName() {
    return "MagicS3GuardCommitter";
  }

  /**
   * Require delayed commit supported in the FS.
   * @return true, always.
   */
  @Override
  protected boolean isDelayedCommitRequired() {
    return true;
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    try (DurationInfo d =
             new DurationInfo("Setup Job %s", jobIdString(context))) {
      Path jobAttemptPath = getJobAttemptPath(context);
      FileSystem fs = getDestination(jobAttemptPath,
          context.getConfiguration());
      if (!fs.mkdirs(jobAttemptPath)) {
        throw new PathCommitException(jobAttemptPath,
            "Failed to mkdir path -it already exists");
      }
    }
  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    LOG.debug("Committing job {}", jobIdString(context));
    // force a check for the job attempt to exist. If it
    // doesn't then either the job wasn't set up, its finished.
    // or something got in the way
    getDestFS().getFileStatus(getJobAttemptPath(context));

    cleanupJob(context);
    maybeCreateSuccessMarker(context);
  }

  /**
   * Cleanup job: abort uploads, delete directories.
   * @param context Job context
   * @throws IOException IO failure
   */
  @Override
  public void cleanupJob(JobContext context) throws IOException {
    try (DurationInfo d =
             new DurationInfo("Aborting outstanding uploads for Job %s",
                 jobIdString(context))) {
      if (getCommitActions() != null) {
        Path pending = getJobAttemptPath(context);
        FileCommitActions.CommitAllFilesOutcome outcome
            = getCommitActions().abortAllSinglePendingCommits(pending, true);
        outcome.maybeRethrow();
      }
    }
    try (DurationInfo d = new DurationInfo("Cleanup job %s",
        jobIdString(context))) {
      deleteWithWarning(getDestFS(),
          getMagicJobAttemptsPath(getOutputPath()), true);
      deleteWithWarning(getDestFS(),
          getTempJobAttemptPath(getAppAttemptId(context),
              getMagicJobAttemptsPath(getOutputPath())), true);
    }
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    try (DurationInfo d = new DurationInfo("Setup Task %s",
        context.getTaskAttemptID())) {
      Path taskAttemptPath = getTaskAttemptPath(context);
      FileSystem fs = taskAttemptPath.getFileSystem(getConf());
      fs.mkdirs(taskAttemptPath);
    }
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
   * This adds four HTTP requests to the call. It may be better just to
   * return true and rely on the commit task doing the work.
   * @param context task context
   * @param taskAttemptPath path to the attempt
   * @return true if the attempt path exists
   * @throws IOException failure to list the path
   */
  private boolean needsTaskCommit(TaskAttemptContext context,
      Path taskAttemptPath) throws IOException {
    try (DurationInfo d =
             new DurationInfo("needsTaskCommit task %s",
                 context.getTaskAttemptID())) {
      if (taskAttemptPath == null) {
        taskAttemptPath = getTaskAttemptPath(context);
      }
      FileSystem fs = taskAttemptPath.getFileSystem(
          context.getConfiguration());
      return fs.exists(taskAttemptPath);
    }
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    try (DurationInfo d = new DurationInfo("Commit task %s",
        context.getTaskAttemptID())) {
      FileCommitActions.CommitAllFilesOutcome outcome =
          innerCommitTask(context);
      outcome.maybeRethrow();
    } finally {
      // delete the task attempt so there's no possibility of a second attempt
      deleteQuietly(getDestFS(), getTaskAttemptPath(context), true);
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
    return getCommitActions().commitSinglePendingCommitFiles(
        getTaskAttemptPath(context), true);
  }

  /**
   * Abort a task. Attempt load then abort all pending files,
   * then try to delete the task attempt path.
   * @param context task context
   * @throws IOException if there was some problem querying the path other
   * than it not actually existing.
   */
  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    Path attemptPath = getTaskAttemptPath(context);
    try (DurationInfo d =
             new DurationInfo("Abort task %s", context.getTaskAttemptID())) {
      FileCommitActions.CommitAllFilesOutcome outcome
          = getCommitActions().abortAllSinglePendingCommits(
          attemptPath, true);
    } finally {
      deleteQuietly(getDestFS(), attemptPath, true);
    }
  }

  /**
   * Compute the path where the output of a given job attempt will be placed.
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path to store job attempt data.
   */
  protected Path getJobAttemptPath(int appAttemptId) {
    return getMagicJobAttemptPath(appAttemptId, getOutputPath());
  }

  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   *
   * @param context the context of the task attempt.
   * @return the path where a task attempt should be stored.
   */
  public Path getTaskAttemptPath(TaskAttemptContext context) {
    return getMagicTaskAttemptPath(context, getOutputPath());
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
}
