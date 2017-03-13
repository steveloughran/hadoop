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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.fs.s3a.commit.CommitUtils.*;

/**
 * Abstract base class for s3guard committers; allows for any commonality
 * between different architectures.
 *
 * Although the committer APIs allow for a committer to be created without
 * an output path, this is no supported in this class or its subclasses:
 * a destination must be supplied. It is left to the committer factory
 * to handle the creation of a committer when the destination is unknown.
 *
 * Requiring an output directory simplifies coding and testing.
 */
public abstract class AbstractS3GuardCommitter extends PathOutputCommitter {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractS3GuardCommitter.class);
  private Path outputPath;
  private Path workPath;
  private Configuration conf;
  private FileSystem destFS;

  /**
   * Create a committer.
   * @param outputPath the job's output path: MUST NOT be null.
   * @param context the job context
   * @throws IOException on a failure
   */
  protected AbstractS3GuardCommitter(Path outputPath,
      JobContext context) throws IOException {
    Preconditions.checkArgument(outputPath != null);
    setConf(context.getConfiguration());
    initOutput(outputPath);
    LOG.debug("Committer instantiated for job \"{}\" ID {} with destination {}",
        context.getJobName(), context.getJobID(), outputPath);
  }

  /** TESTING ONLY; allows mock FS to cheat. */
  protected void initOutput(Path outputPath) throws IOException {
    FileSystem fs = getDestination(outputPath, getConf());
    setDestFS(fs);
    setOutputPath(fs.makeQualified(outputPath));
  }

  /**
   * Create a committer.
   * This constructor binds the destination directory and configuration, but
   * does not update the work path: That must be calculated by the implemenation;
   * its ommitted here to avoid subclass methods being called too early.
   * @param outputPath the job's output path: MUST NOT be null.
   * @param context the task's context
   * @throws IOException on a failure
   */
  protected AbstractS3GuardCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    this(outputPath, (JobContext) context);
    LOG.debug("Committer instantiated for task ID {} for job \"{}\" " +
            "ID {}",
        context.getTaskAttemptID(),
        context.getJobName(), context.getJobID());
  }

  /**
   * Final path of output, in the destination FS.
   * @return the path
   */
  public Path getOutputPath() {
    return outputPath;
  }

  protected void setOutputPath(Path outputPath) {
    Preconditions.checkNotNull(outputPath, "Null output path");
    this.outputPath = outputPath;
  }

  /**
   * This is the critical method for {@code FileOutputFormat}; it declares
   * the path for work
   * @return the working path.
   */
  @Override
  public Path getWorkPath() {
    return workPath;
  }

  protected void setWorkPath(Path workPath) {
    LOG.debug("Setting work path to {}", workPath);
    this.workPath = workPath;
  }

  public Configuration getConf() {
    return conf;
  }

  protected void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Get the destination FS, on demand if it is not already set
   * @return the filesystem; requires the output path to be set up
   * @throws IOException if the FS cannot be instantiated.
   */

  protected FileSystem getDestFS() throws IOException {
    if (destFS == null) {
      FileSystem fs = getDestination(outputPath, getConf());
      setDestFS(fs);
    }
    return destFS;
  }

  /**
   * Get the destination as an S3A Filesystem; casting it.
   * @return the dest S3A FS.
   * @throws IOException if the FS cannot be instantiated.
   */
  protected S3AFileSystem getDestS3AFS() throws IOException {
    return (S3AFileSystem)getDestFS();
  }

  protected void setDestFS(FileSystem destFS) {
    this.destFS = destFS;
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
  protected abstract Path getJobAttemptPath(int appAttemptId);

  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   *
   * @param context the context of the task attempt.
   * @return the path where a task attempt should be stored.
   */
  protected abstract Path getTaskAttemptPath(TaskAttemptContext context);

  /**
   * Get a temporary directory for data. When a task is aborted/cleaned
   * up, the contents of this directory are all deleted.
   * @param context task context
   * @return a path for temporary data.
   */
  public abstract Path getTempTaskAttemptPath(TaskAttemptContext context);

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "AbstractS3GuardCommitter{");
    sb.append("outputPath=").append(getOutputPath());
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
  protected FileSystem getDestination(Path out, Configuration conf)
      throws IOException {
    return getS3AFileSystem(out, conf, isDelayedCommitRequired());
  }

  /**
   * Flag to indicate whether or not the destination filesystem needs
   * to be configured to support the delayed commit mechanism.
   * @return what the requirements of the committer are of the S3 endpoint
   */
  protected abstract boolean isDelayedCommitRequired();

  /**
   * Task recovery considered unsupported: Warn and fail.
   * @param taskContext Context of the task whose output is being recovered
   * @throws IOException always.
   */
  @Override
  public void recoverTask(TaskAttemptContext taskContext) throws IOException {
    LOG.warn("Cannot recover task {}", taskContext.getTaskAttemptID());
    throw new IOException(String.format("Unable to recover task %s",
        taskContext.getTaskAttemptID()));
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    try (DurationInfo d = new DurationInfo("Setup Task %s",
        context.getTaskAttemptID())) {
      Path taskAttemptPath = getTaskAttemptPath(context);
      FileSystem fs = getTaskAttemptFilesystem(context);
      fs.mkdirs(taskAttemptPath);

    }
  }

/**
 * Get the task attempt path filesystem. This may not be the same as the
 * final destination FS, and so may not be an S3A FS.
 * @param context task attempt
 * @return the filesystem
 * @throws IOException failure to instantiate
 */
  protected FileSystem getTaskAttemptFilesystem(TaskAttemptContext context)
      throws IOException {
    Path taskAttemptPath = getTaskAttemptPath(context);
    FileSystem fs = taskAttemptPath.getFileSystem(getConf());
    return taskAttemptPath.getFileSystem(getConf());
  }

}
