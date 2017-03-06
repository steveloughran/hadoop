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
/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.commit.staging;

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.AbstractS3GuardCommitter;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.CommitUtils;
import org.apache.hadoop.fs.s3a.commit.DurationInfo;
import org.apache.hadoop.fs.s3a.commit.FileCommitActions;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingCommitterConstants.*;
import static org.apache.hadoop.fs.s3a.commit.CommitUtils.*;


/**
 * Committer based on the architecture of the
 * <a href="https://github.com/rdblue/s3committer">Netflix multipart committers.</a>
 * <ol>
 *   <li>
 *   The working directory of each task is actually under a temporary
 *   path in the local filesystem; jobs write directly into it.
 *   </li>
 *   <li>
 *     Task Commit: list all files under the task working dir, upload
 *     each of them but do not commit the final operation.
 *     Persist the information for each pending commit into a directory
 *     (ISSUE: which FS?) for enumeration by the job committer.
 *   </li>
 *   <li>Task Abort: recursive delete of task working dir.</li>
 *   <li>Job Commit: list all pending PUTs to commit; commit them.</li>
 *   <li>
 *     Job Abort: list all pending PUTs to commit; abort them.
 *     Delete all task attempt directories.
 *   </li>
 * </ol>
 * <p>
 * Configuration options:
 * <pre>
 *   : temporary local FS directory
 *   : intermediate directory on a cluster-wide FS (can be HDFS or a consistent
 *   s3 endpoint).
 *
 * </pre>
 */

public class StagingS3GuardCommitter extends AbstractS3GuardCommitter {


  private static final Logger LOG = LoggerFactory.getLogger(
      StagingS3GuardCommitter.class);
  private final Path constructorOutputPath;
  private final long uploadPartSize;
  private final String uuid;
  private final Path workPath;

  // lazy variables
  private AmazonS3 client = null;
  private ConflictResolution mode = null;
  private ExecutorService threadPool = null;
  private Path finalOutputPath = null;
  private String bucket = null;
  private String s3KeyPrefix = null;
  private Path bucketRoot = null;
  
  public StagingS3GuardCommitter(Path outputPath,
      JobContext context) throws IOException {
    super(outputPath, context);
    constructorOutputPath = getOutputPath();
    Configuration conf = getConf();
    uploadPartSize = conf.getLong(
        UPLOAD_SIZE, DEFAULT_UPLOAD_SIZE);
    // Spark will use a fake app id based on the current minute and job id 0.
    // To avoid collisions, use the YARN application ID for Spark.
    uuid = getUploadUUID(conf, context.getJobID().toString());
    workPath = buildWorkPath(context);
  }


  public StagingS3GuardCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    constructorOutputPath = getOutputPath();
    Configuration conf = getConf();
    uploadPartSize = conf.getLong(
        UPLOAD_SIZE, DEFAULT_UPLOAD_SIZE);
    // Spark will use a fake app id based on the current minute and job id 0.
    // To avoid collisions, use the YARN application ID for Spark.
    uuid = getUploadUUID(conf, context.getJobID().toString());
    workPath = buildWorkPath(context);
  }


  private static String getUploadUUID(Configuration conf, String jobId) {
    return conf.get(UPLOAD_UUID, conf.get(
        SPARK_WRITE_UUID,
        conf.get(SPARK_APP_ID, jobId)));
  }

  private Path buildWorkPath(JobContext context) throws IOException {
    if (context instanceof TaskAttemptContext) {
      return taskAttemptPath((TaskAttemptContext) context, uuid);
    } else {
      return null;
    }
  }

  private static Path taskAttemptPath(TaskAttemptContext context, String uuid)
  throws IOException {
    return getTaskAttemptPath(context, Paths.getLocalTaskAttemptTempDir(
        context.getConfiguration(), uuid,
        getTaskId(context), getAttemptId(context)));
  }

  private static int getTaskId(TaskAttemptContext context) {
    return context.getTaskAttemptID().getTaskID().getId();
  }

  private static int getAttemptId(TaskAttemptContext context) {
    return context.getTaskAttemptID().getId();
  }

  @Override
  protected boolean isDelayedCommitRequired() {
    return false;
  }

  /**
   * Compute the path where the output of a given job attempt will be placed.
   * @param context the context of the job.  This is used to get the
   * application attempt id.
   * @return the path to store job attempt data.
   */
  public Path getJobAttemptPath(JobContext context) {
    return getJobAttemptPath(context, getOutputPath());
  }

  /**
   * Get the filesystem for the job attempt
   * @param context the context of the job.  This is used to get the
   * application attempt id.
   * @return the FS to store job attempt data.
   */
  public FileSystem getJobAttemptFileSystem(JobContext context) throws IOException {
    Path p = getJobAttemptPath(context);
    return p.getFileSystem(context.getConfiguration());
  }

  /**
   * Compute the path where the output of a given job attempt will be placed.
   * @param context the context of the job.  This is used to get the
   * application attempt id.
   * @param out the output path to place these in.
   * @return the path to store job attempt data.
   */
  public static Path getJobAttemptPath(JobContext context, Path out) {
    return getJobAttemptPath(getAppAttemptId(context), out);
  }

  /**
   * Compute the path where the output of a given job attempt will be placed.
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path to store job attempt data.
   */
  protected Path getJobAttemptPath(int appAttemptId) {
    return getJobAttemptPath(appAttemptId, getOutputPath());
  }

  /**
   * Compute the path where the output of a given job attempt will be placed.
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path to store job attempt data.
   */
  private static Path getJobAttemptPath(int appAttemptId, Path out) {
    return new Path(getPendingJobAttemptsPath(out),
        String.valueOf(appAttemptId));
  }

  /**
   * Compute the path where the output of pending task attempts are stored.
   * @param context the context of the job with pending tasks.
   * @return the path where the output of pending task attempts are stored.
   */
  private Path getPendingTaskAttemptsPath(JobContext context) {
    return getPendingTaskAttemptsPath(context, getOutputPath());
  }

  /**
   * Compute the path where the output of pending task attempts are stored.
   * @param context the context of the job with pending tasks.
   * @return the path where the output of pending task attempts are stored.
   */
  private static Path getPendingTaskAttemptsPath(JobContext context, Path out) {
    return new Path(getJobAttemptPath(context, out),
        CommitConstants.PENDING_DIR_NAME);
  }

  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   *
   * @param context the context of the task attempt.
   * @return the path where a task attempt should be stored.
   */
  @Override
  public Path getTaskAttemptPath(TaskAttemptContext context) {
    return new Path(getPendingTaskAttemptsPath(context),
        String.valueOf(context.getTaskAttemptID()));
  }

  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   *
   * @param context the context of the task attempt.
   * @param out The output path to put things in.
   * @return the path where a task attempt should be stored.
   */
  public static Path getTaskAttemptPath(TaskAttemptContext context, Path out) {
    return new Path(getPendingTaskAttemptsPath(context, out),
        String.valueOf(context.getTaskAttemptID()));
  }

  /**
   * @return the path where the output of pending job attempts are
   * stored.
   */
  private Path getPendingJobAttemptsPath() {
    return getPendingJobAttemptsPath(getOutputPath());
  }

  /**
   * Get the location of pending job attempts.
   * @param out the base output directory.
   * @return the location of pending job attempts.
   */
  private static Path getPendingJobAttemptsPath(Path out) {
    return new Path(out, CommitConstants.PENDING_DIR_NAME);
  }

  /**
   * Compute the path where the output of a committed task is stored until
   * the entire job is committed.
   * @param context the context of the task attempt
   * @return the path where the output of a committed task is stored until
   * the entire job is committed.
   */
  public Path getCommittedTaskPath(TaskAttemptContext context) {
    return getCommittedTaskPath(CommitUtils.getAppAttemptId(context), context);
  }

  public static Path getCommittedTaskPath(TaskAttemptContext context,
      Path out) {
    return getCommittedTaskPath(getAppAttemptId(context), context, out);
  }

  /**
   * Compute the path where the output of a committed task is stored until the
   * entire job is committed for a specific application attempt.
   * @param appAttemptId the id of the application attempt to use
   * @param context the context of any task.
   * @return the path where the output of a committed task is stored.
   */
  protected Path getCommittedTaskPath(int appAttemptId,
      TaskAttemptContext context) {
    return new Path(getJobAttemptPath(appAttemptId),
        String.valueOf(context.getTaskAttemptID().getTaskID()));
  }

  private static Path getCommittedTaskPath(int appAttemptId,
      TaskAttemptContext context,
      Path out) {
    return new Path(getJobAttemptPath(appAttemptId, out),
        String.valueOf(context.getTaskAttemptID().getTaskID()));
  }

  @Override
  public Path getTempTaskAttemptPath(TaskAttemptContext context) {
    throw new UnsupportedOperationException("Unimplemented");
  }

  /**
   * Getter for the cached {@link AmazonS3} client. Subclasses should call this
   * method to get a client instance.
   *
   * @param path the output S3 path (with bucket)
   * @param conf a Hadoop {@link Configuration}
   * @return a {@link AmazonS3} client
   */
  protected AmazonS3 getClient(Path path, Configuration conf)
      throws IOException {
    if (client != null) {
      return client;
    }
    S3AFileSystem fs = getS3AFileSystem(path, conf, false);
    client = fs.getAmazonS3Client();
    return client;
  }

  /**
   * Lists the output of a task under the task attempt path. Subclasses can
   * override this method to change how output files are identified.
   * <p>
   * This implementation lists the files that are direct children of the output
   * path and filters hidden files (file names starting with '.' or '_').
   * <p>
   * The task attempt path is provided by
   * {@link #getTaskAttemptPath(TaskAttemptContext)}
   *
   * @param context this task's {@link TaskAttemptContext}
   * @return the output files produced by this task in the task attempt path
   * @throws IOException
   */
  protected Iterable<FileStatus> getTaskOutput(TaskAttemptContext context)
      throws IOException {
    // get files on the local FS in the attempt path
    Path attemptPath = getTaskAttemptPath(context);
    FileSystem attemptFS = attemptPath.getFileSystem(
        context.getConfiguration());
    FileStatus[] stats = attemptFS.listStatus(
        attemptPath, HiddenPathFilter.get());
    return Arrays.asList(stats);
  }

  /**
   * Returns the final S3 key for a relative path. Subclasses can override this
   * method to upload files to a different S3 location.
   * <p>
   * This implementation concatenates the relative path with the key prefix
   * from the output path.
   *
   * @param relative the path of a file relative to the task attempt path
   * @param context the JobContext or TaskAttemptContext for this job
   * @return the S3 key where the file will be uploaded
   */
  protected String getFinalKey(String relative, JobContext context)
      throws IOException {
    return getS3KeyPrefix(context) + "/" + Paths.addUUID(relative, uuid);
  }

  /**
   * Returns the final S3 location for a relative path as a Hadoop {@link Path}.
   * This is a final method that calls {@link #getFinalKey(String, JobContext)}
   * to determine the final location.
   *
   * @param relative the path of a file relative to the task attempt path
   * @param context the JobContext or TaskAttemptContext for this job
   * @return the S3 Path where the file will be uploaded
   */
  protected final Path getFinalPath(String relative, JobContext context)
      throws IOException {
    return new Path(getBucketRoot(context), getFinalKey(relative, context));
  }

  /**
   * Returns the target output path based on the output path passed to the
   * constructor.
   * <p>
   * Subclasses can override this method to redirect output. For example, a
   * committer can write output to a new directory instead of deleting the
   * current directory's contents, and then point a table to the new location.
   *
   * @param outputPath the output path passed to the constructor
   * @param context the JobContext passed to the constructor
   * @return the final output path
   */
  protected Path getFinalOutputPath(Path outputPath, JobContext context) {
    return outputPath;
  }

  protected final Path getOutputPath(JobContext context) throws IOException {
    if (finalOutputPath == null) {
      this.finalOutputPath = getFinalOutputPath(constructorOutputPath, context);
      Preconditions.checkNotNull(finalOutputPath, "Output path cannot be null");

      URI outputUri = URI.create(finalOutputPath.toString());
      S3AFileSystem fs = getS3AFileSystem(finalOutputPath,
          context.getConfiguration(), false);
      this.bucket = fs.getBucket();
      this.s3KeyPrefix = fs.pathToKey(finalOutputPath);
    }
    return finalOutputPath;
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    context.getConfiguration().set(UPLOAD_UUID, uuid);
  }

  protected List<S3Util.PendingUpload> getPendingUploads(JobContext context)
      throws IOException {
    return getPendingUploads(context, false);
  }

  protected List<S3Util.PendingUpload> getPendingUploadsIgnoreErrors(
      JobContext context) throws IOException {
    return getPendingUploads(context, true);
  }

  private List<S3Util.PendingUpload> getPendingUploads(
      JobContext context, boolean suppressExceptions) throws IOException {

    Path jobAttemptPath = getJobAttemptPath(context);  //wrappedCommitter.getJobAttemptPath(context);
    final FileSystem attemptFS = jobAttemptPath.getFileSystem(
        context.getConfiguration());
    FileStatus[] pendingCommitFiles = attemptFS.listStatus(
        jobAttemptPath, HiddenPathFilter.get());

    final List<S3Util.PendingUpload> pending = Lists.newArrayList();

    // try to read every pending file and add all results to pending.
    // in the case of a failure to read the file, exceptions are held until all
    // reads have been attempted.
    Tasks.foreach(pendingCommitFiles)
        .throwFailureWhenFinished(!suppressExceptions)
        .executeWith(getThreadPool(context))
        .run(new Tasks.Task<FileStatus, IOException>() {
          @Override
          public void run(FileStatus pendingCommitFile) throws IOException {
            pending.addAll(S3Util.readPendingCommits(
                attemptFS, pendingCommitFile.getPath()));
          }
        });

    return pending;
  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    commitJobInternal(context, getPendingUploads(context));
  }

  protected void commitJobInternal(JobContext context,
      List<S3Util.PendingUpload> pending)
      throws IOException {
    final AmazonS3 client = getClient(
        getOutputPath(context), context.getConfiguration());

    boolean threw = true;
    try {
      Tasks.foreach(pending)
          .stopOnFailure().throwFailureWhenFinished()
          .executeWith(getThreadPool(context))
          .onFailure(
              new Tasks.FailureTask<S3Util.PendingUpload, RuntimeException>() {
                @Override
                public void run(S3Util.PendingUpload commit,
                    Exception exception) {
                  S3Util.abortCommit(client, commit);
                }
              })
          .abortWith(new Tasks.Task<S3Util.PendingUpload, RuntimeException>() {
            @Override
            public void run(S3Util.PendingUpload commit) {
              S3Util.abortCommit(client, commit);
            }
          })
          .revertWith(new Tasks.Task<S3Util.PendingUpload, RuntimeException>() {
            @Override
            public void run(S3Util.PendingUpload commit) {
              S3Util.revertCommit(client, commit);
            }
          })
          .run(new Tasks.Task<S3Util.PendingUpload, RuntimeException>() {
            @Override
            public void run(S3Util.PendingUpload commit) {
              S3Util.finishCommit(client, commit);
            }
          });

      threw = false;

    } finally {
      cleanup(context, threw);
    }
  }

  @Override
  public void abortJob(JobContext context, JobStatus.State state)
      throws IOException {
    List<S3Util.PendingUpload> pending = getPendingUploadsIgnoreErrors(context);
    abortJobInternal(context, pending, false);
  }

  protected void abortJobInternal(JobContext context,
      List<S3Util.PendingUpload> pending,
      boolean suppressExceptions)
      throws IOException {
    final AmazonS3 client = getClient(
        getOutputPath(context), context.getConfiguration());

    boolean threw = true;
    try {
      Tasks.foreach(pending)
          .throwFailureWhenFinished(!suppressExceptions)
          .executeWith(getThreadPool(context))
          .onFailure(
              new Tasks.FailureTask<S3Util.PendingUpload, RuntimeException>() {
                @Override
                public void run(S3Util.PendingUpload commit,
                    Exception exception) {
                  S3Util.abortCommit(client, commit);
                }
              })
          .run(new Tasks.Task<S3Util.PendingUpload, RuntimeException>() {
            @Override
            public void run(S3Util.PendingUpload commit) {
              S3Util.abortCommit(client, commit);
            }
          });

      threw = false;

    } finally {
      cleanup(context, threw || suppressExceptions);
    }
  }

  private void cleanup(JobContext context, boolean suppressExceptions)
      throws IOException {
    cleanupJob(context);
  }


  @Override
  public void cleanupJob(JobContext context) throws IOException {
    try (DurationInfo d = new DurationInfo("Cleanup job %s",
        context.getJobID())) {
      deleteWithWarning(getJobAttemptFileSystem(context),
          getJobAttemptPath(context), true);
/*
      deleteWithWarning(getDestFS(),
          getTempJobAttemptPath(getAppAttemptId(context),
              getMagicJobAttemptsPath(getOutputPath())), true);
*/
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

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context)
      throws IOException {
    try (DurationInfo d = new DurationInfo("needsTaskCommit() Task %s",
        context.getTaskAttemptID())) {
      // check for files on the local FS in the attempt path
      Path attemptPath = getTaskAttemptPath(context);
      FileSystem fs = getTaskAttemptFilesystem(context);

      // This could be made more efficient with a probe "hasChildren(Path)"
      // which returns true if there is >1 entry under a given path.
      FileStatus[] stats = fs.listStatus(attemptPath);
      LOG.debug("{} files to commit", stats.length);
      return stats.length > 0;
    } catch (FileNotFoundException e) {
      // list didn't find a directory, so nothing to commit
      LOG.debug("No files to commit");
      return false;
    }

  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    try (DurationInfo d = new DurationInfo("commit task %s",
        context.getTaskAttemptID())) {
      Iterable<FileStatus> stats = getTaskOutput(context);
      commitTaskInternal(context, stats);
    }
  }

  protected void commitTaskInternal(final TaskAttemptContext context,
      Iterable<FileStatus> taskOutput)
      throws IOException {
    Configuration conf = context.getConfiguration();
    final AmazonS3 client = getClient(getOutputPath(context), conf);

    final Path attemptPath = getTaskAttemptPath(context);
    FileSystem attemptFS = attemptPath.getFileSystem(conf);

    // add the commits file to the wrapped commiter's task attempt location.
    // this complete file will be committed by the wrapped committer at the end
    // of this method.
    Path commitsAttemptPath = getTaskAttemptPath(context);
    FileSystem commitsFS = getTaskAttemptFilesystem(context);

    // keep track of unfinished commits in case one fails. if something fails,
    // we will try to abort the ones that had already succeeded.
    final List<S3Util.PendingUpload> commits = Lists.newArrayList();


    boolean threw = true;
    ObjectOutputStream completeUploadRequests = new ObjectOutputStream(
        commitsFS.create(commitsAttemptPath, false));
    try {
      Tasks.foreach(taskOutput)
          .stopOnFailure().throwFailureWhenFinished()
          .executeWith(threadPool)
          .run(new Tasks.Task<FileStatus, IOException>() {
            @Override
            public void run(FileStatus stat) throws IOException {
              File localFile = new File(
                  URI.create(stat.getPath().toString()).getPath());
              if (localFile.length() <= 0) {
                return;
              }
              String relative = Paths.getRelativePath(
                  attemptPath, stat.getPath());
              String partition = getPartition(relative);
              String key = getFinalKey(relative, context);
              S3Util.PendingUpload commit = S3Util.multipartUpload(client,
                  localFile, partition, getBucket(context), key,
                  uploadPartSize);
              commits.add(commit);
            }
          });

      for (S3Util.PendingUpload commit : commits) {
        completeUploadRequests.writeObject(commit);
      }

      threw = false;

    } finally {
      if (threw) {
        Tasks.foreach(commits)
            .run(new Tasks.Task<S3Util.PendingUpload, RuntimeException>() {
              @Override
              public void run(S3Util.PendingUpload commit) {
                S3Util.abortCommit(client, commit);
              }
            });
        try {
          attemptFS.delete(attemptPath, true);
        } catch (Exception e) {
          LOG.error("Failed while cleaning up failed task commit: ", e);
        }
      }
      Closeables.close(completeUploadRequests, threw);
    }

    // TODO
    //  wrappedCommitter.commitTask(context);

    attemptFS.delete(attemptPath, true);
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
    return getTaskAttemptPath(context).getFileSystem(context.getConfiguration());
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    // the API specifies that the task has not yet been committed, so there are
    // no uploads that need to be cancelled. just delete files on the local FS.
    try (DurationInfo d =
             new DurationInfo("Abort task %s", context.getTaskAttemptID())) {
      Path attemptPath = getTaskAttemptPath(context);
      FileSystem taskFS = getTaskAttemptFilesystem(context);
      deleteQuietly(taskFS, attemptPath, true);
      deleteQuietly(taskFS, getTempTaskAttemptPath(context), true);
    }
  }
  /**
   * Returns the partition of a relative file path, or null if the path is a
   * file name with no relative directory.
   *
   * @param relative a relative file path
   * @return the partition of the relative file path
   */
  protected final String getPartition(String relative) {
    return Paths.getParent(relative);
  }

  private final String getBucket(JobContext context) throws IOException {
    if (bucket == null) {
      // getting the output path sets the bucket from the path
      getOutputPath(context);
    }
    return bucket;
  }

  private String getS3KeyPrefix(JobContext context) throws IOException {
    if (s3KeyPrefix == null) {
      // getting the output path sets the s3 key prefix from the path
      getOutputPath(context);
    }
    return s3KeyPrefix;
  }

  protected String getUUID() {
    return uuid;
  }

  /**
   * Returns an {@link ExecutorService} for parallel tasks. The number of
   * threads in the thread-pool is set by s3.multipart.committer.num-threads.
   * If num-threads is 0, this will return null;
   *
   * @param context the JobContext for this commit
   * @return an {@link ExecutorService} or null for the number of threads
   */
  protected final ExecutorService getThreadPool(JobContext context) {
    if (threadPool == null) {
      int numThreads = context.getConfiguration().getInt(
          NUM_THREADS, DEFAULT_NUM_THREADS);
      LOG.debug("Createing thread pool of size {}", numThreads);
      if (numThreads > 0) {
        this.threadPool = Executors.newFixedThreadPool(numThreads,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("s3-committer-pool-%d")
                .build());
      } else {
        return null;
      }
    }
    return threadPool;
  }


  /**
   * Returns the bucket root of the output path.
   *
   * @param context the JobContext for this commit
   * @return a Path that is the root of the output bucket
   */
  protected final Path getBucketRoot(JobContext context) throws IOException {
    if (bucketRoot == null) {
      this.bucketRoot = Paths.getRoot(getOutputPath(context));
    }
    return bucketRoot;
  }

  /**
   * Returns the {@link ConflictResolution} mode for this commit.
   *
   * @param context the JobContext for this commit
   * @return the ConflictResolution mode
   */
  protected final ConflictResolution getMode(JobContext context) {
    if (mode == null) {
      this.mode = ConflictResolution.valueOf(context
          .getConfiguration()
          .get(CONFLICT_MODE, "fail")
          .toUpperCase(Locale.ENGLISH));
    }
    return mode;
  }
}
