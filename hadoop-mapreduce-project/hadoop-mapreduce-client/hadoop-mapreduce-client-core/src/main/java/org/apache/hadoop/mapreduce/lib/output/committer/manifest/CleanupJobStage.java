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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.functional.RemoteIterators;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED_DEFAULT;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED_DEFAULT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_CLEANUP_MOVE_TO_TRASH;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_CLEANUP_MOVE_TO_TRASH_DEFAULT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_CLEANUP_PARALLEL_ATTEMPT_DIRS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_CLEANUP_PARALLEL_ATTEMPT_DIRS_DEFAULT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_CLEANUP;

/**
 * Clean up a job deleting the job directory.
 * Returns: whether or not the delete was attempted, the target directory.
 */
public class CleanupJobStage extends
    AbstractJobCommitStage<
        CleanupJobStage.Options,
        CleanupJobStage.CleanupResult> {

  private static final Logger LOG = LoggerFactory.getLogger(
      CleanupJobStage.class);

  public CleanupJobStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_CLEANUP, true);
  }

  @Override
  protected CleanupJobStage.CleanupResult executeStage(CleanupJobStage.Options options)
      throws IOException {
    final Path dir = requireNonNull(getStageConfig().getOutputTempSubDir());
    LOG.info("Cleaup of directory {} with ", dir, options);
    if (!options.enabled) {
      LOG.info("Cleanup of {} disabled", dir);
      return new CleanupJobStage.CleanupResult(dir, true, false, 0);
    }
    boolean shouldDelete = !options.moveToTrash;
    boolean wasRenamed = false;
    if (!shouldDelete) {
      // to rename
      LOG.info("Moving temporary directory to trash {}", dir);
      wasRenamed = moveOutputTemporaryDirToTrash();
      // if it did not work, fall back to a real delete
      shouldDelete = !wasRenamed;
      if (shouldDelete) {
        LOG.warn("Rename to trash failed; trying to delete it instead.");
      }
    }
    if (shouldDelete){
      // to delete.

      LOG.info("Deleting job directory {}", dir);
      if (options.deleteTaskAttemptDirsInParallel) {
        // Attempt to do a parallel delete of task attempt dirs;
        // don't overreact if a delete fails, but stop trying
        // to delete the others, and fall back to deleting the
        // job dir.
        try {
          RemoteIterator<FileStatus> it;
          it = RemoteIterators.filteringRemoteIterator(
              listStatusIterator(getStageConfig().getJobAttemptTaskSubDir()),
              st -> st.isDirectory());
          List<FileStatus> taskAttemptDirs = RemoteIterators.toList(it);
          getIOStatistics().aggregate((retrieveIOStatistics(it)));
          LOG.info("Attempting Parallel deletion of {} task attempt dir(s)",
              taskAttemptDirs.size());
          TaskPool.foreach(taskAttemptDirs)
              .executeWith(getIOProcessors())
              .stopOnFailure()
              .run(status -> deleteDir(status.getPath(), false));
        } catch (FileNotFoundException ignored) {
          // not a problem if there's no dir to list.
        } catch (IOException e) {
          // failure. Log and continue
          LOG.warn("Exception while listing/deleting task attempt directories",
              e);
        }
      }
      // And finish with the top-level deletion.
      deleteDir(dir, options.suppressExceptions);
    }
    return new CleanupJobStage.CleanupResult(dir, false, wasRenamed, 1);
  }

  /**
   * Options to pass down to the cleanup stage.
   */
  public static class Options {

    /** Delete is enabled? */
    private final boolean enabled;

    /** Attempt parallel delete of task attempt dirs? */
    private final boolean deleteTaskAttemptDirsInParallel;

    /** Ignore failures? */
    private final boolean suppressExceptions;

    /** Rather than delete: move to trash? */
    private final boolean moveToTrash;

    public Options(
        final boolean enabled,
        final boolean deleteTaskAttemptDirsInParallel,
        final boolean suppressExceptions,
        final boolean moveToTrash) {
      this.enabled = enabled;
      this.deleteTaskAttemptDirsInParallel = deleteTaskAttemptDirsInParallel;
      this.suppressExceptions = suppressExceptions;
      this.moveToTrash = moveToTrash;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("Options{");
      sb.append("enabled=").append(enabled);
      sb.append(", deleteTaskAttemptDirsInParallel=")
          .append(deleteTaskAttemptDirsInParallel);
      sb.append(", suppressExceptions=").append(suppressExceptions);
      sb.append(", moveToTrash=").append(moveToTrash);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Build an options argument from a configuration, using the
   * settings from FileOutputCommitter and manifest committer.
   * @param conf configuration to use.
   * @return the options to process
   */
  public static CleanupJobStage.Options cleanupStageOptionsFromConfig(
      Configuration conf) {

    boolean enabled = !conf.getBoolean(FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED,
        FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED_DEFAULT);
      boolean suppressExceptions = conf.getBoolean(
          FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED,
          FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED_DEFAULT);
      boolean moveToTrash = conf.getBoolean(
          OPT_CLEANUP_MOVE_TO_TRASH,
          OPT_CLEANUP_MOVE_TO_TRASH_DEFAULT);
      boolean deleteTaskAttemptDirsInParallel = conf.getBoolean(
          OPT_CLEANUP_PARALLEL_ATTEMPT_DIRS,
          OPT_CLEANUP_PARALLEL_ATTEMPT_DIRS_DEFAULT);
    return new CleanupJobStage.Options(
        enabled,
        deleteTaskAttemptDirsInParallel,
        suppressExceptions,
        moveToTrash);
  }

  /**
   * Result of the cleanup
   */
  public static class CleanupResult {
    private final Path directory;
    private final boolean wasSkipped;
    private final boolean wasRenamed;
    private final int deleteCalls;

    public CleanupResult(final Path directory,
        final boolean wasSkipped,
        final boolean wasRenamed,
        final int deleteCalls) {
      this.directory = directory;
      this.wasSkipped = wasSkipped;
      this.wasRenamed = wasRenamed;
      this.deleteCalls = deleteCalls;
    }

    public Path getDirectory() {
      return directory;
    }

    public boolean wasRenamed() {
      return wasRenamed;
    }

    public boolean wasSkipped() {
      return wasSkipped;
    }

    public int getDeleteCalls() {
      return deleteCalls;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "CleanupResult{");
      sb.append("directory=").append(directory);
      sb.append(", wasSkipped=").append(wasSkipped);
      sb.append(", wasRenamed=").append(wasRenamed);
      sb.append(", deleteCalls=").append(deleteCalls);
      sb.append('}');
      return sb.toString();
    }
  }
}
