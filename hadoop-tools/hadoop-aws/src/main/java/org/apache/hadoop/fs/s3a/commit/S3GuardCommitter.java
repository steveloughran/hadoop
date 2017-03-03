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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;

/**
 * This is a dedicated committer which only works with consistent
 * S3A FileSystems.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class S3GuardCommitter extends AbstractS3GuardCommitter {
  private static final Logger LOG =
      LoggerFactory.getLogger(S3GuardCommitter.class);

  /**
   * Name of this class: {@value}.
   */
  public static final String NAME
      = "org.apache.hadoop.fs.s3a.commit.S3GuardCommitter";

  private FileCommitActions commitActions;

  /**
   * Instantiate.
   * @param outputPath output path; may be null.
   * @param context job context
   * @throws IOException
   */
  public S3GuardCommitter(Path outputPath,
      JobContext context) throws IOException {
    super(outputPath, context);
    commitActions = new FileCommitActions(getDestFS());
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
    super(outputPath, context);
    commitActions = new FileCommitActions(getDestFS());
    setWorkPath(getTaskAttemptPath(context));
    verifyIsDelayedCommitPath(getDestFS(), getWorkPath());
    LOG.debug("Task attempt {} has work path {}",
        context.getTaskAttemptID(),
        getWorkPath());
  }

  @Override
  protected boolean isDelayedCommitRequired() {
    return true;
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    try (DurationInfo d =
             new DurationInfo("Setup Job %s", context.getJobID())) {
      Path jobAttemptPath = getJobAttemptPath(context);
      S3AFileSystem fs = getDestination(jobAttemptPath,
          context.getConfiguration());
      if (!fs.mkdirs(jobAttemptPath)) {
        throw new PathCommitException(jobAttemptPath,
            "Failed to mkdir path -it already exists");
      }
    }
  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    LOG.debug("Committing job {}", context.getJobID());
    // force a check for the job attempt to exist. If it
    // doesn't then either the job wasn't set up, its finished.
    // or something got in the way
    getDestFS().getFileStatus(getJobAttemptPath(context));

    cleanupJob(context);

    // True if the job requires output.dir marked on successful job.
    // Note that by default it is set to true.
    if (context.getConfiguration().getBoolean(
        SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)) {
      commitActions.touchSuccessMarker(getOutputPath());
    }
  }

  @Override
  public void abortJob(JobContext context, JobStatus.State state)
      throws IOException {
    try (DurationInfo d = new DurationInfo("Abort Job %s in state %s",
        context.getJobID(), state)) {
      if (commitActions != null) {
        Path pending = pendingSubdir(getOutputPath());
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
    try (DurationInfo d = new DurationInfo("Cleanup job %s",
        context.getJobID())) {
      deleteWithWarning(getDestFS(),
          getPendingJobAttemptsPath(getOutputPath()), true);
      deleteWithWarning(getDestFS(),
          getTempJobAttemptPath(getAppAttemptId(context),
              getPendingJobAttemptsPath(getOutputPath())), true);
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
   * This adds 4 HTTP requests to the call. It may be better just to
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
    return commitActions.commitAllPendingFilesInPath(
        getTaskAttemptPath(context), true);
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    Path attemptPath = getTaskAttemptPath(context);
    try (DurationInfo d =
             new DurationInfo("Abort task %s", context.getTaskAttemptID())) {
      FileCommitActions.CommitAllFilesOutcome outcome
          = commitActions.abortAllPendingFilesInPath(
          attemptPath, true);
      outcome.maybeRethrow();
    } finally {
      deleteQuietly(getDestFS(), attemptPath, true);
      deleteQuietly(getDestFS(), getTempTaskAttemptPath(context), true);
    }
  }


}
