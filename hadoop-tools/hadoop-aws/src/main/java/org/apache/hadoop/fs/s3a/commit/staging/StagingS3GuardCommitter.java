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

package org.apache.hadoop.fs.s3a.commit.staging;

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.mapreduce.JobID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.AbstractS3GuardCommitter;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.DurationInfo;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

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
 * Committer based on the contributed work of the
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
  private final Boolean uniqueFilenames;
  private final FileOutputCommitter wrappedCommitter;

  // lazy variables
  private AmazonS3 client = null;
  private ConflictResolution conflictResolution = null;
  private ExecutorService threadPool = null;
  private Path finalOutputPath = null;
  private String bucket = null;
  private String s3KeyPrefix = null;
  private Path bucketRoot = null;

  /** The directory in the cluster FS for commits to go to */
  private Path commitsDirectory;

  public StagingS3GuardCommitter(Path outputPath, JobContext context)
      throws IOException {
    super(outputPath, context);
    constructorOutputPath = getOutputPath();
    Preconditions.checkNotNull(constructorOutputPath, "output path");
    Configuration conf = getConf();
    this.uploadPartSize = conf.getLong(UPLOAD_SIZE, DEFAULT_UPLOAD_SIZE);
    // Spark will use a fake app id based on the current minute and job id 0.
    // To avoid collisions, use the YARN application ID for Spark.
    this.uuid = getUploadUUID(conf, context.getJobID());
    this.uniqueFilenames = conf.getBoolean(COMMITTER_UNIQUE_FILENAMES,
        DEFAULT_COMMITTER_UNIQUE_FILENAMES);
    setWorkPath(buildWorkPath(context, uuid));
    this.wrappedCommitter = createWrappedCommitter(context, conf);
    postCreationActions();
  }

  public StagingS3GuardCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    constructorOutputPath = getOutputPath();
    Preconditions.checkNotNull(constructorOutputPath, "output path");
    Configuration conf = getConf();
    this.uploadPartSize = conf.getLong(UPLOAD_SIZE, DEFAULT_UPLOAD_SIZE);
    // Spark will use a fake app id based on the current minute and job id 0.
    // To avoid collisions, use the YARN application ID for Spark.
    this.uuid = getUploadUUID(conf, context.getJobID());
    this.uniqueFilenames = conf.getBoolean(COMMITTER_UNIQUE_FILENAMES,
        DEFAULT_COMMITTER_UNIQUE_FILENAMES);
    setWorkPath(buildWorkPath(context, uuid));
    this.wrappedCommitter = createWrappedCommitter(context, conf);
    postCreationActions();
  }

  private void postCreationActions() throws IOException {
    // forces evaluation and caching of the resolution mode.
    ConflictResolution mode = getConflictResolutionMode(getJobContext());
    LOG.debug("Conflict resolution mode: {}", mode);
  }

  /**
   * Create the wrapped committer.
   * This includes customizing its options, and setting up the destination
   * directory.
   * @param context job/task context.
   * @param conf config
   * @return the inner committer
   * @throws IOException
   */
  protected FileOutputCommitter createWrappedCommitter(JobContext context,
      Configuration conf) throws IOException {

    // explicitly choose commit algorithm
    initFileOutputCommitterOptions(context);
    commitsDirectory = Paths.getMultipartUploadCommitsDirectory(conf, uuid);
    return new FileOutputCommitter(commitsDirectory, context);
  }

  /**
   * Init the context config with everything needed for the file output
   * committer. In particular, this code currently only works with
   * commit algorithm 1.
   * @param context context to configure.
   */
  protected void initFileOutputCommitterOptions(JobContext context) {
    context.getConfiguration()
        .setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "StagingS3GuardCommitter{");
    sb.append("constructorOutputPath=").append(constructorOutputPath);
    sb.append(", conflictResolution=").append(conflictResolution);
    sb.append(", finalOutputPath=").append(finalOutputPath);
    sb.append(' ');
    sb.append(super.toString());
    sb.append('}');
    return sb.toString();
  }

  /**
   * Get the UUID of an upload; may be the job ID.
   * @param conf job/task configuration
   * @param jobId Job ID
   * @return an ID for use in paths.
   */
  public static String getUploadUUID(Configuration conf, String jobId) {
    return conf.getTrimmed(UPLOAD_UUID,
        conf.get(SPARK_WRITE_UUID,
            conf.getTrimmed(SPARK_APP_ID, jobId)));
  }
  /**
   * Get the UUID of an upload; may be the job ID.
   * @param conf job/task configuration
   * @param jobId Job ID
   * @return an ID for use in paths.
   */
  public static String getUploadUUID(Configuration conf, JobID jobId) {
    return getUploadUUID(conf, jobId.toString());
  }

  /**
   * Get the work path for a task.
   * @param context job/task complex
   * @param uuid UUID
   * @return a path or null if the context is not of a task
   * @throws IOException failure to build the path
   */
  private static Path buildWorkPath(JobContext context, String uuid)
      throws IOException {
    if (context instanceof TaskAttemptContext) {
      return taskAttemptWorkingPath((TaskAttemptContext) context, uuid);
    } else {
      return null;
    }
  }

  @Override
  protected boolean isDelayedCommitRequired() {
    return false;
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
  private static Path getJobAttemptPath(int appAttemptId, Path out) {
    return new Path(getPendingJobAttemptsPath(out),
        String.valueOf(appAttemptId));
  }

  @Override
  protected Path getJobAttemptPath(int appAttemptId) {
    //TODO Is this valid?
    return new Path(getPendingJobAttemptsPath(commitsDirectory),
        String.valueOf(appAttemptId));
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
   * @param out The output path to put things in.
   * @return the path where a task attempt should be stored.
   */
  public static Path getTaskAttemptPath(TaskAttemptContext context, Path out) {
    return new Path(getPendingTaskAttemptsPath(context, out),
        String.valueOf(context.getTaskAttemptID()));
  }

  /**
   * Get the location of pending job attempts.
   * @param out the base output directory.
   * @return the location of pending job attempts.
   */
  private static Path getPendingJobAttemptsPath(Path out) {
    Preconditions.checkNotNull(out, "Null 'out' path");
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
    return getCommittedTaskPath(getAppAttemptId(context), context);
  }

  private static void validateContext(TaskAttemptContext context) {
    Preconditions.checkNotNull(context, "null context");
    Preconditions.checkNotNull(context.getTaskAttemptID(),
        "null task attempt ID");
    Preconditions.checkNotNull(context.getTaskAttemptID().getTaskID(),
        "null task ID");
    Preconditions.checkNotNull(context.getTaskAttemptID().getJobID(),
        "null job ID");
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
    validateContext(context);
    return new Path(getJobAttemptPath(appAttemptId),
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
   * @throws IOException on a failure
   */
  protected List<FileStatus> getTaskOutput(TaskAttemptContext context)
      throws IOException {
    // get files on the local FS in the attempt path
    Path workPath = getWorkPath();
    Preconditions.checkNotNull(workPath, "No work path in {}", this);
    FileSystem attemptFS = workPath.getFileSystem(context.getConfiguration());
    LOG.debug("Scanning {} for files to commit", workPath);
    try {
      FileStatus[] stats = attemptFS.listStatus(workPath,
          Paths.HiddenPathFilter.get());
      return Arrays.asList(stats);
    } catch (FileNotFoundException e) {
      LOG.debug("No output generated in task");
//      return new ArrayList<>(0);
      throw (FileNotFoundException) new FileNotFoundException(
          String.format("No local working directory %s for task %s",
              workPath, context.getTaskAttemptID().toString())).initCause(e);

    }
  }

  /**
   * Returns the final S3 key for a relative path. Subclasses can override this
   * method to upload files to a different S3 location.
   * <p>
   * This implementation concatenates the relative path with the key prefix
   * from the output path.
   * If {@link StagingCommitterConstants#COMMITTER_UNIQUE_FILENAMES} is
   * set, then the task UUID is also included in the calculation
   *
   * @param relative the path of a file relative to the task attempt path
   * @param context the JobContext or TaskAttemptContext for this job
   * @return the S3 key where the file will be uploaded
   */
  protected String getFinalKey(String relative, JobContext context)
      throws IOException {
    if (uniqueFilenames) {
      return getS3KeyPrefix(context) + "/" + Paths.addUUID(relative, uuid);
    } else {
      return getS3KeyPrefix(context) + "/" + relative;
    }
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

  // TODO
  /*
  @Override
  public Path getCommittedTaskPath(TaskAttemptContext context) {
    // return the location in HDFS where the multipart upload context is
    return wrappedCommitter.getCommittedTaskPath(context);
  }
  */

  @Override
  public Path getTaskAttemptPath(TaskAttemptContext context) {
    // a path on the local FS for files that will be uploaded
    return getWorkPath();
  }

  @Override
  public Path getJobAttemptPath(JobContext context) {
    return wrappedCommitter.getJobAttemptPath(context);
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

  @Override
  public void setupJob(JobContext context) throws IOException {
    LOG.debug("Setting up job {}", context.getJobID());
    context.getConfiguration().set(UPLOAD_UUID, uuid);
    wrappedCommitter.setupJob(context);
  }

  /**
   * Get the list of pending uploads for this job attempt
   * @param context job context
   * @return a list of pending uploads.
   * @throws IOException Any IO failure
   */
  protected List<S3Util.PendingUpload> getPendingUploads(JobContext context)
      throws IOException {
    return getPendingUploads(context, false);
  }

  /**
   * Get the list of pending uploads for this job attempt, swallowing
   * exceptions.
   * @param context job context
   * @return a list of pending uploads. If an exception was swallowed,
   * then this may not match the actual set of pending operations
   * @throws IOException shouldn't be raised, but retained for compiler
   */
  protected List<S3Util.PendingUpload> getPendingUploadsIgnoreErrors(
      JobContext context) throws IOException {
    return getPendingUploads(context, true);
  }

  /**
   * Get the list of pending uploads for this job attempt.
   * @param context job context
   * @param suppressExceptions should exceptions be swallowed?
   * @return a list of pending uploads. If exceptions are being swallowed,
   * then this may not match the actual set of pending operations
   * @throws IOException Any IO failure which wasn't swallowed.
   */
  private List<S3Util.PendingUpload> getPendingUploads(
      JobContext context, boolean suppressExceptions) throws IOException {
    Path jobAttemptPath = wrappedCommitter.getJobAttemptPath(context);
    final FileSystem attemptFS = jobAttemptPath.getFileSystem(
        context.getConfiguration());
    final List<S3Util.PendingUpload> pending = Lists.newArrayList();
    FileStatus[] pendingCommitFiles;
    try {
      pendingCommitFiles = attemptFS.listStatus(
          jobAttemptPath, Paths.HiddenPathFilter.get());
    } catch (IOException e) {
      // unable to work with endpoint, if suppressing errors decide our actions
      if (suppressExceptions) {
        LOG.info("Failed to list pending upload dir", e);
        return pending;
      } else {
        throw e;
      }
    }

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
    try (DurationInfo d = new DurationInfo("Commit Job %s",
        context.getJobID())) {
      List<S3Util.PendingUpload> pending = getPendingUploads(context);
      commitJobInternal(context, pending);
    }
  }

  protected void commitJobInternal(JobContext context,
                                   List<S3Util.PendingUpload> pending)
      throws IOException {
    final AmazonS3 client = getClient(
        getOutputPath(context), context.getConfiguration());

    if (pending.isEmpty()) {
      LOG.warn("No pending uploads to commit");
    }
    LOG.debug("Commiting the output of {} task(s)", pending.size());
    boolean threw = true;
    try {
      Tasks.foreach(pending)
          .stopOnFailure().throwFailureWhenFinished()
          .executeWith(getThreadPool(context))
          .onFailure(new Tasks.FailureTask<S3Util.PendingUpload, IOException>() {
            @Override
            public void run(S3Util.PendingUpload commit,
                            Exception exception) throws IOException {
              S3Util.abortCommit(client, commit);
            }
          })
          .abortWith(new Tasks.Task<S3Util.PendingUpload, IOException>() {
            @Override
            public void run(S3Util.PendingUpload commit) throws IOException {
              S3Util.abortCommit(client, commit);
            }
          })
          .revertWith(new Tasks.Task<S3Util.PendingUpload, IOException>() {
            @Override
            public void run(S3Util.PendingUpload commit) throws IOException {
              S3Util.revertCommit(client, commit);
            }
          })
          .run(new Tasks.Task<S3Util.PendingUpload, IOException>() {
            @Override
            public void run(S3Util.PendingUpload commit) throws IOException {
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
    try (DurationInfo d = new DurationInfo("Abort Job %s in state %s ",
        context.getJobID(), state)) {
      List<S3Util.PendingUpload> pending = getPendingUploadsIgnoreErrors(context);
      abortJobInternal(context, pending, false);
    }
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
          .onFailure(new Tasks.FailureTask<S3Util.PendingUpload, IOException>() {
            @Override
            public void run(S3Util.PendingUpload commit,
                            Exception exception) throws IOException {
              S3Util.abortCommit(client, commit);
            }
          })
          .run(new Tasks.Task<S3Util.PendingUpload, IOException>() {
            @Override
            public void run(S3Util.PendingUpload commit) throws IOException {
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

    if (suppressExceptions) {
      try {
        wrappedCommitter.cleanupJob(context);
      } catch (Exception e) {
        LOG.error("Failed while cleaning up job", e);
      }
    } else {
      wrappedCommitter.cleanupJob(context);
    }
    deleteDestinationPaths(context);
  }


  @Override
  public void cleanupJob(JobContext context) throws IOException {
    try (DurationInfo d = new DurationInfo("Cleanup Job %s",
        context.getJobID())) {
      cleanup(context, false);
    }
  }

  protected void deleteDestinationPaths(JobContext context) throws IOException {
    deleteWithWarning(getJobAttemptFileSystem(context),
        getJobAttemptPath(context), true);
    // delete the __temporary directory. This will cause problems
    // if there is >1 task targeting the same dest dir
    deleteWithWarning(getDestFS(),
        new Path(getOutputPath(context), CommitConstants.PENDING_DIR_NAME),
        true);
    // and the working path
    deleteTaskWorkingPathQuietly(context);
  }


  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
// TODO
    Path taskAttemptPath = getTaskAttemptPath(context);
    try (DurationInfo d = new
        DurationInfo("task %s: creating task attempt path %s ",
        taskAttemptPath,
        context.getTaskAttemptID())) {
      // create the local FS
      taskAttemptPath.getFileSystem(getConf()).mkdirs(taskAttemptPath);
      wrappedCommitter.setupTask(context);
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
      // TODO: throw this up as an error?
      LOG.info("No files to commit");
      return false;
    }
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    try (DurationInfo d = new DurationInfo("Commit task %s",
        context.getTaskAttemptID())) {
      try {
        List<FileStatus> filesToCommit = getTaskOutput(context);
        int count = commitTaskInternal(context, filesToCommit);
        LOG.info("Committed file count: {}", count);
      } catch (IOException e) {
        LOG.error("Commit of task {} failed",
            context.getTaskAttemptID(), e);
        throw e;
      }
    }
  }

  /**
   * Commit the task by uploading all created files and then
   * writing a pending entry for them.
   * @param context task context
   * @param taskOutput list of files from the output
   * @throws IOException IO Failures.
   */
  protected int commitTaskInternal(final TaskAttemptContext context,
      List<FileStatus> taskOutput)
      throws IOException {
    Configuration conf = context.getConfiguration();

    final Path attemptPath = getTaskAttemptPath(context);
    FileSystem attemptFS = getTaskAttemptFilesystem(context);
    LOG.debug("Attempt path is {}", attemptPath);


    // add the commits file to the wrapped committer's task attempt location.
    // this complete file will be committed by the wrapped committer at the end
    // of this method.
    Path commitsAttemptPath = wrappedCommitter.getTaskAttemptPath(context);
    FileSystem commitsFS = commitsAttemptPath.getFileSystem(conf);

    // keep track of unfinished commits in case one fails. if something fails,
    // we will try to abort the ones that had already succeeded.
    final List<S3Util.PendingUpload> commits = Lists.newArrayList();

    LOG.info("Uploading from staging directory to destination filesystem");
    LOG.info("Saving pending data information to {}", commitsAttemptPath);
    if (taskOutput.isEmpty()) {
      // there is nothing to write. needsTaskCommit() should have caught
      // this, so warn that there is some kind of problem in the protocol.
      LOG.warn("No files to commit");
      return 0;
    }

    final AmazonS3 amazonS3 = getClient(getOutputPath(context), conf);
    boolean threw = true;
    // before the uploads, report some progress
    context.progress();

    // although overwrite=false, there's still a risk of > 1 entry being
    // committed if the FS doesn't have create-no-overwrite consistency.
    ObjectOutputStream completeUploadRequests = new ObjectOutputStream(
        commitsFS.create(commitsAttemptPath, false));
    try {
      Tasks.foreach(taskOutput)
          .stopOnFailure()
          .throwFailureWhenFinished()
          .executeWith(threadPool)
          .run(new Tasks.Task<FileStatus, IOException>() {
            @Override
            public void run(FileStatus stat) throws IOException {
              File localFile = new File(
                  URI.create(stat.getPath().toString()).getPath());
/* 0 byte files are still PUTtable
              if (localFile.length() <= 0) {
                return;
              }
*/
              String relative = Paths.getRelativePath(attemptPath, stat.getPath());
              String partition = getPartition(relative);
              String key = getFinalKey(relative, context);
              S3Util.PendingUpload commit = S3Util.multipartUpload(amazonS3,
                  localFile, partition, getBucket(context), key,
                  uploadPartSize);
              LOG.debug("Adding pending commit {}", commit);
              commits.add(commit);
            }
          });

      for (S3Util.PendingUpload commit : commits) {
        completeUploadRequests.writeObject(commit);
      }

      threw = false;

    } finally {
      if (threw) {
        LOG.error("Exception during commit process, aborting {} commit(s)",
          commits.size());
        Tasks.foreach(commits)
            .run(new Tasks.Task<S3Util.PendingUpload, IOException>() {
              @Override
              public void run(S3Util.PendingUpload commit) throws IOException {
                S3Util.abortCommit(amazonS3, commit);
              }
            });
        deleteTaskAttemptPathQuietly(context);
      }
      Closeables.close(completeUploadRequests, threw);
    }

    // TODO: what else is needed here?
    wrappedCommitter.commitTask(context);

    attemptFS.delete(attemptPath, true);
    return commits.size();
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    // the API specifies that the task has not yet been committed, so there are
    // no uploads that need to be cancelled. just delete files on the local FS.
    try (DurationInfo d =
             new DurationInfo("Abort task %s", context.getTaskAttemptID())) {
      deleteTaskAttemptPathQuietly(context);
      deleteTaskWorkingPathQuietly(context);
      wrappedCommitter.abortTask(context);
    }
  }

  /**
   * Get the work path for a task.
   * @param context job/task complex
   * @param uuid UUID
   * @return a path
   * @throws IOException failure to build the path
   */
  private static Path taskAttemptWorkingPath(TaskAttemptContext context,
      String uuid) throws IOException {
    return getTaskAttemptPath(context,
        Paths.getLocalTaskAttemptTempDir(
            context.getConfiguration(),
            uuid,
            context.getTaskAttemptID()));
  }

  static int getTaskId(TaskAttemptContext context) {
    return context.getTaskAttemptID().getTaskID().getId();
  }

  static int getAttemptId(TaskAttemptContext context) {
    return context.getTaskAttemptID().getId();
  }

  protected void deleteTaskAttemptPathQuietly(TaskAttemptContext context)
      throws IOException {
    Path attemptPath = getTaskAttemptPath(context);
    try {
      FileSystem taskFS = getTaskAttemptFilesystem(context);
      deleteQuietly(taskFS, attemptPath, true);
    } catch (IOException e) {
      LOG.debug("Failed to delete task attempt path {}", attemptPath, e);
    }
  }

  protected void deleteTaskWorkingPathQuietly(JobContext context)
      throws IOException {
    Path path = buildWorkPath(context, getUUID());
    if (path != null) {
      try {
        deleteQuietly(path.getFileSystem(getConf()), path, true);
      } catch (IOException e) {
        LOG.debug("Failed to delete path {}",path, e);
      }
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

  protected final Path getOutputPath(JobContext context) throws IOException {
    if (finalOutputPath == null) {
      this.finalOutputPath = getFinalOutputPath(constructorOutputPath, context);
      Preconditions.checkNotNull(finalOutputPath, "Output path cannot be null");

      S3AFileSystem fs = getS3AFileSystem(finalOutputPath,
          context.getConfiguration(), false);
      this.bucket = fs.getBucket();
      this.s3KeyPrefix = fs.pathToKey(finalOutputPath);
      LOG.debug("Final output path is {}", finalOutputPath);
    }
    return finalOutputPath;
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
          COMMITTER_THREADS, DEFAULT_COMMITTER_THREADS);
      LOG.debug("Creating thread pool of size {}", numThreads);
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
  public final ConflictResolution getConflictResolutionMode(JobContext context) {
    if (conflictResolution == null) {
      this.conflictResolution = ConflictResolution.valueOf(
          getConfictModeOption(context));
    }
    return conflictResolution;
  }

  /**
   * Get the conflict mode option string
   * @param context context with the config
   * @return the trimmed configuration option, upper case.
   */
  public static final String getConfictModeOption(JobContext context) {
    return context
        .getConfiguration()
        .getTrimmed(CONFLICT_MODE, DEFAULT_CONFLICT_MODE)
        .toUpperCase(Locale.ENGLISH);
  }
}
