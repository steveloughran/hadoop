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

import com.amazonaws.services.s3.model.MultipartUpload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.fs.contract.ContractTestUtils.listChildren;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.FilterTempFiles;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.lsR;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test the job/task commit actions of an S3A Committer, including trying to
 * simulate some failure and retry conditions.
 * Derived from
 * {@code org.apache.hadoop.mapreduce.lib.output.TestFileOutputCommitter}.
 */
@SuppressWarnings("unchecked")
public abstract class AbstractITCommitProtocol extends AbstractCommitITest {
  protected Path outDir;
  public static final String COMMIT_FAILURE_MESSAGE = "oops";

  private static final String SUB_DIR = "SUB_DIR";

  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractITCommitProtocol.class);

  // A random task attempt id for testing.
  protected static final String ATTEMPT_0 =
      "attempt_200707121733_0001_m_000000_0";
  protected static final String PART_00000 = "part-m-00000";
  protected static final TaskAttemptID TASK_ATTEMPT_0 =
      TaskAttemptID.forName(ATTEMPT_0);

  protected static final String ATTEMPT_1 =
      "attempt_200707121733_0001_m_000001_0";
  protected static final TaskAttemptID TASK_ATTEMPT_1 =
      TaskAttemptID.forName(ATTEMPT_1);

  private Text key1 = new Text("key1");
  private Text key2 = new Text("key2");
  private Text val1 = new Text("val1");
  private Text val2 = new Text("val2");

  private void cleanupDestDir() throws IOException {
    rmdir(this.outDir, getConfiguration());
  }

  public void rmdir(Path dir, Configuration conf) throws IOException {
    describe("deleting %s", dir);
    FileSystem fs = dir.getFileSystem(conf);
    fs.delete(dir, true);
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    outDir = path(getMethodName());
    S3AFileSystem fileSystem = getFileSystem();
    bindFileSystem(fileSystem, outDir, fileSystem.getConf());
    cleanupDestDir();
  }

  @Override
  public void teardown() throws Exception {
    abortMultipartUploadsUnderPath(outDir);
    cleanupDestDir();
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
//    disableFilesystemCaching(conf);
    return conf;
  }

  /**
   * Bind a path to the FS in the cache.
   * @param fs filesystem
   * @param path s3 path
   * @param conf configuration
   * @throws IOException any problem
   */
  private void bindFileSystem(FileSystem fs, Path path, Configuration conf)
      throws IOException {
    FileSystemTestHelper.addFileSystemForTesting(path.toUri(), conf, fs);
  }

  /**
   * Create a committer for a task.
   * @param context task context
   * @return new committer
   * @throws IOException failure
   */

  protected abstract AbstractS3GuardCommitter createCommitter(
      TaskAttemptContext context) throws IOException;

  /**
   * Create a committer for a job.
   * @param context job context
   * @return new committer
   * @throws IOException failure
   */
  public abstract AbstractS3GuardCommitter createCommitter(
      JobContext context) throws IOException;

  protected void writeTextOutput(TaskAttemptContext context)
      throws IOException, InterruptedException {
    describe("write output");
    try (DurationInfo d = new DurationInfo("Writing Text output for task %s",
        context.getTaskAttemptID())) {
      writeOutput(new LoggingTextOutputFormat().getRecordWriter(context),
          context);
    }
  }

  private void writeOutput(RecordWriter theRecordWriter,
      TaskAttemptContext context) throws IOException, InterruptedException {
    NullWritable nullWritable = NullWritable.get();
    try {
      theRecordWriter.write(key1, val1);
      theRecordWriter.write(null, nullWritable);
      theRecordWriter.write(null, val1);
      theRecordWriter.write(nullWritable, val2);
      theRecordWriter.write(key2, nullWritable);
      theRecordWriter.write(key1, null);
      theRecordWriter.write(null, null);
      theRecordWriter.write(key2, val2);
    } finally {
      theRecordWriter.close(context);
    }
  }

  private void writeMapFileOutput(RecordWriter theRecordWriter,
      TaskAttemptContext context) throws IOException, InterruptedException {
    describe("\nWrite map output");
    try (DurationInfo d = new DurationInfo("Writing Text output for task %s",
        context.getTaskAttemptID())) {
      for (int i = 0; i < 10; ++i) {
        Text val = ((i & 1) == 1) ? val1 : val2;
        theRecordWriter.write(new LongWritable(i), val);
      }
    } finally {
      theRecordWriter.close(context);
    }
  }

  @Test
  public void testRecovery() throws Exception {
    Job job = newJob();
    Configuration conf = job.getConfiguration();
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, ATTEMPT_0);
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
    JobContext jContext = new JobContextImpl(conf, TASK_ATTEMPT_0.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf,
        TASK_ATTEMPT_0);
    AbstractS3GuardCommitter committer = createCommitter(tContext);

    // setup
    setup(committer, jContext, tContext);

    // write output
    writeTextOutput(tContext);

    // do commit
    committer.commitTask(tContext);
    Path attemptPath = committer.getTaskAttemptPath(tContext);
    assertPathDoesNotExist("commit dir", attemptPath);

 /*   Path jobTempDir1 = committer.getCommittedTaskPath(tContext);
    File jtd = new File(jobTempDir1.toUri().getPath());
    int commitVersion = 2;
    int recoveryVersion = 2;
    if (commitVersion == 1) {
      assertTrue("Version 1 commits to temporary dir " + jtd, jtd.exists());
      validateContent(jtd);
    } else {
      assertFalse("Version 2 commits to output dir " + jtd, jtd.exists());
    }
*/
    //now while running the second app attempt,
    //recover the task output from first attempt
    Configuration conf2 = job.getConfiguration();
    conf2.set(MRJobConfig.TASK_ATTEMPT_ID, ATTEMPT_0);
    conf2.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 2);
    JobContext jContext2 = new JobContextImpl(conf2, TASK_ATTEMPT_0.getJobID());
    TaskAttemptContext tContext2 = new TaskAttemptContextImpl(conf2,
        TASK_ATTEMPT_0);
    AbstractS3GuardCommitter committer2 = createCommitter(tContext2);
    committer2.setupJob(tContext2);

    assertFalse("recoverySupported in " + committer2,
        committer2.isRecoverySupported());
    intercept(IOException.class,
        () -> committer2.recoverTask(tContext2));

    // at this point, task attempt 0 has failed to recover
    // it should be abortable though.
    committer2.abortTask(tContext2);
/*

    committer2.commitJob(jContext2);
    validateContent(outDir, shouldExpectSuccessMarker());
*/
  }

  private void validateContent(Path dir, boolean expectSuccessMarker)
      throws IOException {
    if (expectSuccessMarker) {
      assertSuccessMarkerExists(dir);
    }
    Path expectedFile = new Path(dir, PART_00000);
    LOG.debug("Validating content in {}", expectedFile);
    assertPathExists("Output file", expectedFile);
    StringBuffer expectedOutput = new StringBuffer();
    expectedOutput.append(key1).append('\t').append(val1).append("\n");
    expectedOutput.append(val1).append("\n");
    expectedOutput.append(val2).append("\n");
    expectedOutput.append(key2).append("\n");
    expectedOutput.append(key1).append("\n");
    expectedOutput.append(key2).append('\t').append(val2).append("\n");
    String output = slurp(expectedFile);
    assertEquals("Content of " + expectedFile,
        expectedOutput.toString(), output);
  }

  /**
   * Look for the partFile subdir of the output dir.
   * @param fs filesystem
   * @param dir output dir
   * @throws IOException IO failure.
   */
  private void validateMapFileOutputContent(
      FileSystem fs, Path dir) throws IOException {
    // map output is a directory with index and data files
    assertPathExists("Map output", dir);
    Path expectedMapDir = new Path(dir, PART_00000);
    assertPathExists("Map output", expectedMapDir);
    assertIsDirectory(expectedMapDir);
    FileStatus[] files = fs.listStatus(expectedMapDir);
    assertTrue("No files found in " + expectedMapDir, files.length > 0);
    assertPathExists("index file in " + expectedMapDir,
        new Path(expectedMapDir, MapFile.INDEX_FILE_NAME));
    assertPathExists("data file in " + expectedMapDir,
        new Path(expectedMapDir, MapFile.DATA_FILE_NAME));
  }

  /**
   * Dump all uploads.   */
  protected void dumpMultipartUploads() throws IOException {
    countMultipartUploads("");
  }

  protected void assertMultipartUploadsPending(Path path) throws IOException {
    int count = countMultipartUploads(path);
    assertTrue("No multipart uploads in progress under " + path, count > 0);
  }

  protected void assertNoMultipartUploadsPending(Path path) throws IOException {
    int count = countMultipartUploads(path);
    assertEquals("Multipart uploads in progress under " + path, 0, count);
  }

  protected int countMultipartUploads(Path path) throws IOException {
    return countMultipartUploads(path == null ? "" :
        getFileSystem().pathToKey(path));
  }

  protected int countMultipartUploads(String prefix) throws IOException {
    int count = 0;
    for (MultipartUpload upload : getFileSystem().listMultipartUploads(
        prefix)) {
      count++;
      LOG.info("Upload {} to {}", upload.getUploadId(), upload.getKey());
    }
    return count;
  }

  /**
   * Abort all multipart uploads under a path.
   * @param key key/prefix
   * @return a count of aborts
   * @throws IOException trouble.
   */
  protected int abortMultipartUploadsUnderPath(Path path) throws IOException {
    int count = 0;
    S3AFileSystem fs = getFileSystem();
    String key = fs.pathToKey(path);
    S3AFileSystem.WriteOperationHelper writeOps
        = fs.createWriteOperationHelper(key);
    for (MultipartUpload upload : fs.listMultipartUploads(key)) {
      count++;
      LOG.info("Aborting upload {} to {}",
          upload.getUploadId(), upload.getKey());
      try {
        writeOps.abortMultipartCommit(upload);
      } catch (FileNotFoundException e) {
        LOG.info("Already aborted: {}", upload.getKey(), e);
      }
    }
    if (count > 0) {
      LOG.info("Multipart uploads deleted: {}", count);
    }
    return count;
  }


  @Test
  public void testCommitter() throws Exception {
    Job job = newJob();
    Configuration conf = job.getConfiguration();
    JobContext jContext = new JobContextImpl(conf, TASK_ATTEMPT_0.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf,
        TASK_ATTEMPT_0);
    AbstractS3GuardCommitter committer = createCommitter(tContext);

    // setup
    setup(committer, jContext, tContext);

    try {
      // write output
      describe("1. Writing output");
      writeTextOutput(tContext);

      dumpMultipartUploads();
      describe("2. Committing task");
      assertTrue("No files to commit were found by " + committer,
          committer.needsTaskCommit(tContext));
      committer.commitTask(tContext);

      // this is only task commit; there MUST be no part- files in the dest dir
      try {
        RemoteIterator<LocatedFileStatus> files
            = getFileSystem().listFiles(outDir, false);
        S3ATestUtils.iterateOverFiles(files,
            (LocatedFileStatus status) -> {
              assertFalse("task committed file to dest :" + status,
                  status.getPath().toString().contains("part"));
            });
      } catch (FileNotFoundException e) {
        LOG.info("Outdir {} is not created by task commit phase ",
            outDir);
      }

      describe("3. Committing job");
      committer.commitJob(jContext);
      describe("4. Validating content");

      // validate output
      validateContent(outDir, shouldExpectSuccessMarker());
      assertNoMultipartUploadsPending(outDir);
    } finally {
      abort(committer, jContext, tContext);
    }
  }

  /**
   * Create a a new job. Sets the task attempt ID.
   * @return the new job
   * @throws IOException failure
   */
  public Job newJob() throws IOException {
    Job job = Job.getInstance(getConfiguration());
    Configuration conf = job.getConfiguration();
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, ATTEMPT_0);
    conf.setBoolean(CREATE_SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true);
    FileOutputFormat.setOutputPath(job, outDir);
    return job;
  }

  @Test
  public void testCommitterWithDuplicatedCommit() throws Exception {
    Job job = newJob();
    Configuration conf = job.getConfiguration();
    JobContext jContext = new JobContextImpl(conf, TASK_ATTEMPT_0.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf,
        TASK_ATTEMPT_0);
    AbstractS3GuardCommitter committer = createCommitter(tContext);

    // setup
    setup(committer, jContext, tContext);

    // write output
    writeTextOutput(tContext);
    // do commit
    commit(committer, jContext, tContext);

    // validate output
    validateContent(outDir, shouldExpectSuccessMarker());

    assertNoMultipartUploadsPending(outDir);

    // commit task to fail on retry
    expectFNFEonTaskCommit(committer, tContext);
  }

  protected boolean shouldExpectSuccessMarker() {
    return true;
  }

  /**
   * Simulate a failure on the first job commit; expect the
   * second to succeed.
   */
  @Test
  public void testCommitterWithFailure() throws Exception {
    describe("Fail the first job commit then retry");
    Job job = newJob();
    Configuration conf = job.getConfiguration();
    JobContext jContext = new JobContextImpl(conf, TASK_ATTEMPT_0.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf,
        TASK_ATTEMPT_0);
    AbstractS3GuardCommitter committer =
        createFailingCommitter(tContext);

    // setup
    setup(committer, jContext, tContext);

    // write output
    writeTextOutput(tContext);

    // do commit
    committer.commitTask(tContext);

    // now fail job
    expectSimulatedFailureOnJobCommit(jContext, committer);

    // but the data got there, due to the order of operations.
    validateContent(outDir, shouldExpectSuccessMarker());
    expectSecondJobCommitToFail(jContext, committer);

  }

  /**
   * Override point: the failure expected on the attempt to commit a failed
   * job.
   * @param jContext job context
   * @param committer committer
   * @throws Exception any unexpected failure.
   */
  protected IOException expectSecondJobCommitToFail(JobContext jContext,
      AbstractS3GuardCommitter committer) throws Exception {
    // next attempt will fail as there is no longer a directory to commit
    return expectJobCommitFailure(jContext, committer,
        FileNotFoundException.class);
  }

  /**
   * Expect a job commit operation to fail with a specific exception.
   * @param jContext job context
   * @param committer committer
   * @param clazz class of exception
   * @return the caught exception
   * @throws Exception any unexpected failure.
   */
  protected IOException expectJobCommitFailure(JobContext jContext,
      AbstractS3GuardCommitter committer, Class<? extends IOException> clazz)
      throws Exception {
    return intercept(clazz,
        () -> committer.commitJob(jContext));
  }

  /**
   * Create a committer which fails; the class {@link FailThenSucceed}
   * implements the logic.
   * @param tContext task context
   * @return committer instance
   * @throws IOException failure to instantiate
   */
  protected abstract AbstractS3GuardCommitter createFailingCommitter(
      TaskAttemptContext tContext) throws IOException;

  protected void expectFNFEonTaskCommit(AbstractS3GuardCommitter committer,
      TaskAttemptContext tContext) throws Exception {
    intercept(FileNotFoundException.class,
        () -> committer.commitTask(tContext));
  }

  protected void assertSuccessMarkerExists() throws IOException {
    assertSuccessMarkerExists(outDir);
  }

  protected void assertSuccessMarkerExists(Path dir) throws IOException {
    assertPathExists("Success marker",
        new Path(dir, SUCCESS_FILE_NAME));
  }

  protected void assertSuccessMarkerDoesNotExist(Path dir) throws IOException {
    assertPathDoesNotExist("Success marker",
        new Path(dir, SUCCESS_FILE_NAME));
  }

  /**
   * Simulate a failure on the first job commit; expect the
   * second to succeed.
   */
  @Test
  public void testCommitterWithNoOutputs() throws Exception {
    describe("Have a task and job with no outputs: expect success");

    Job job = newJob();
    Configuration conf = job.getConfiguration();

    JobContext jContext = new JobContextImpl(conf, TASK_ATTEMPT_0.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf,
        TASK_ATTEMPT_0);
    AbstractS3GuardCommitter committer =
        createFailingCommitter(tContext);

    // setup
    setup(committer, jContext, tContext);

    // do commit
    committer.commitTask(tContext);
    assertPathDoesNotExist("job path", committer.getTaskAttemptPath(tContext));

    // simulated failure
    expectSimulatedFailureOnJobCommit(jContext, committer);

    assertPathDoesNotExist("job path", committer.getJobAttemptPath(jContext));

    // next attempt will fail
    expectSecondJobCommitToFail(jContext, committer);

  }

  protected static void expectSimulatedFailureOnJobCommit(JobContext jContext,
      AbstractS3GuardCommitter committer) throws Exception {
    intercept(IOException.class,
        COMMIT_FAILURE_MESSAGE,
        () -> {
          committer.commitJob(jContext);
          return "committed job";
        });
  }

  @Test
  public void testMapFileOutputCommitter() throws Exception {
    Job job = newJob();
    FileOutputFormat.setOutputPath(job, outDir);
    Configuration conf = job.getConfiguration();
    JobContext jContext = new JobContextImpl(conf, TASK_ATTEMPT_0.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf,
        TASK_ATTEMPT_0);
    AbstractS3GuardCommitter committer = createCommitter(tContext);

    // setup
    setup(committer, jContext, tContext);

    // write output
    writeMapFileOutput(new MapFileOutputFormat().getRecordWriter(tContext),
        tContext);

    // do commit
    commit(committer, jContext, tContext);
    S3AFileSystem fs = getFileSystem();
    lsR(fs, outDir, true);
    String ls = ls(outDir);
    describe("\nvalidating");

    // validate output
    assertSuccessMarkerExists(outDir);
    describe("validate output of %s", outDir);
    validateMapFileOutputContent(fs, outDir);

    // Ensure getReaders call works and also ignores
    // hidden filenames (_ or . prefixes)
    describe("listing");
    FileStatus[] filtered = fs.listStatus(outDir,
        new FilterTempFiles());
    assertEquals("listed children under " + ls,
        1, filtered.length);
    FileStatus fileStatus = filtered[0];
    assertTrue("Not the part file: " + fileStatus,
        fileStatus.getPath().toString().endsWith(PART_00000));

    describe("getReaders()");
    assertEquals("Number of MapFile.Reader entries with shared FS "
            + outDir + " : " + ls,
        1, getReaders(fs, outDir, conf).length);

    describe("getReaders(new FS)");
    FileSystem fs2 = FileSystem.get(outDir.toUri(), conf);
    assertEquals("Number of MapFile.Reader entries with shared FS2 "
            + outDir + " : " + ls,
        1, getReaders(fs2, outDir, conf).length);

    describe("MapFileOutputFormat.getReaders");
    assertEquals("Number of MapFile.Reader entries with new FS in "
            + outDir + " : " + ls,
        1, MapFileOutputFormat.getReaders(outDir, conf).length);
  }

  /** Open the output generated by this format. */
  private static MapFile.Reader[] getReaders(FileSystem fs,
      Path dir,
      Configuration conf) throws IOException {
    PathFilter filter = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        String name = path.getName();
        if (name.startsWith("_") || name.startsWith(".")) {
          return false;
        }
        return true;
      }
    };
    Path[] names = FileUtil.stat2Paths(fs.listStatus(dir, filter));

    // sort names, so that hash partitioning works
    Arrays.sort(names);

    MapFile.Reader[] parts = new MapFile.Reader[names.length];
    for (int i = 0; i < names.length; i++) {
      parts[i] = new MapFile.Reader(fs, names[i].toString(), conf);
    }
    return parts;
  }

  /**
   * Set up the job and task.
   * @param committer committer
   * @param jContext job context
   * @param tContext task context
   * @throws IOException problems
   */
  protected void setup(AbstractS3GuardCommitter committer,
      JobContext jContext,
      TaskAttemptContext tContext) throws IOException {
    describe("\nsetup job");
    try (DurationInfo d = new DurationInfo("setup job %s",
        jContext.getJobID())) {
      committer.setupJob(jContext);
    }
    try (DurationInfo d = new DurationInfo("setup task %s",
        tContext.getTaskAttemptID())) {
      committer.setupTask(tContext);
    }
    describe("setup complete\n");

    // also: clean the test dir
  }

  protected void abort(AbstractS3GuardCommitter committer,
      JobContext jContext,
      TaskAttemptContext tContext) throws IOException {
    describe("\naborting task");
    try {
      committer.abortTask(tContext);
    } catch (IOException e) {
      LOG.warn("Exception aborting task:", e);
    }
    describe("\naborting job");
    try {
      committer.abortJob(jContext, JobStatus.State.KILLED);
    } catch (IOException e) {
      LOG.warn("Exception aborting job", e);
    }
  }

  /**
   * Set up the task and then the job.
   * @param committer committer
   * @param jContext job context
   * @param tContext task context
   * @throws IOException problems
   */
  protected void commit(AbstractS3GuardCommitter committer,
      JobContext jContext,
      TaskAttemptContext tContext) throws IOException {
    describe("\ncommitting task");
    committer.commitTask(tContext);
    describe("\ncommitting job");
    committer.commitJob(jContext);
    describe("commit complete\n");
  }

  @Test
  public void testAbort() throws Exception {
    Job job = newJob();
    FileOutputFormat.setOutputPath(job, outDir);
    Configuration conf = job.getConfiguration();
    JobContext jContext = new JobContextImpl(conf, TASK_ATTEMPT_0.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf,
        TASK_ATTEMPT_0);
    AbstractS3GuardCommitter committer = createCommitter(tContext);

    // do setup
    setup(committer, jContext, tContext);

    // write output
    writeTextOutput(tContext);

    // do abort
    committer.abortTask(tContext);
    Path expectedPath = new Path(committer.getWorkPath(), PART_00000);
    assertPathNotFound(conf, expectedPath, "task temp dir ");


    committer.abortJob(jContext, JobStatus.State.FAILED);
    Path pendingDir = new Path(outDir, MAGIC_DIR_NAME);
    assertPathNotFound(conf, pendingDir, "job temp dir ");
    S3AFileSystem fs = getFileSystem();
    try {
      FileStatus[] children = listChildren(fs, outDir);
      if (children.length != 0) {
        lsR(fs, outDir, true);
      }
      assertArrayEquals("Output directory not empty " + ls(outDir),
          new FileStatus[0], children);
    } catch (FileNotFoundException e) {
      // this is a valid failure mode; it means the dest dir doesn't exist yet.
    }
  }

  protected void assertPathNotFound(Configuration conf,
      Path expectedPath,
      String message) throws IOException {
    ContractTestUtils.assertPathDoesNotExist(expectedPath.getFileSystem(conf),
        message, expectedPath);
  }

  @Test
  public void testFailAbort() throws Exception {
    Job job = newJob();
    Configuration conf = job.getConfiguration();
    FileOutputFormat.setOutputPath(job, outDir);
    JobContext jContext = new JobContextImpl(conf, TASK_ATTEMPT_0.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf,
        TASK_ATTEMPT_0);
    AbstractS3GuardCommitter committer = createCommitter(tContext);

    // do setup
    setup(committer, jContext, tContext);

    // write output
    writeTextOutput(tContext);

    // do abort
    committer.abortTask(tContext);

    Path jtd = committer.getJobAttemptPath(jContext);
    Path ttd = committer.getTaskAttemptPath(tContext);
    Path expectedFile = new Path(outDir, PART_00000);
    assertPathDoesNotExist("expected output file", expectedFile);
    assertSuccessMarkerDoesNotExist(outDir);
    describe("Aborting job into %s", outDir);

    committer.abortJob(jContext, JobStatus.State.FAILED);

    assertPathDoesNotExist("job temp dir", jtd);

    // try again; expect abort to be idempotent.
    committer.abortJob(jContext, JobStatus.State.FAILED);
    assertNoMultipartUploadsPending(outDir);

  }

  /*
  static class RLFS extends RawLocalFileSystem {
    private final ThreadLocal<Boolean> needNull = new ThreadLocal<Boolean>() {
      @Override
      protected Boolean initialValue() {
        return true;
      }
    };

    public RLFS() {
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      if (needNull.get() &&
          OUT_SUB_DIR.toUri().getPath().equals(f.toUri().getPath())) {
        needNull.set(false); // lie once per thread
        return null;
      }
      return super.getFileStatus(f);
    }
  }
*/

  /**
   * This looks at what happens with concurrent commits.
   * However, the failure condition it looks for (subdir under subdir)
   * is the kind of failure you see on a rename-based commit.
   *
   * What it will not detect is the fact that both tasks will each commit
   * to the destination directory. That is: whichever commits last wins.
   *
   * There's no way to stop this. Instead it is a requirement that the task
   * commit operation is only executed when the committer is happy to
   * commit only those tasks which it knows have succeeded, and abort those
   * which have not.
   * @throws Exception failure
   */
  @Test
  public void testConcurrentCommitTaskWithSubDir() throws Exception {
    Job job = newJob();
    FileOutputFormat.setOutputPath(job, outDir);
    final Configuration conf = job.getConfiguration();
/*

    conf.setClass("fs.file.impl", RLFS.class, FileSystem.class);
    FileSystem.closeAll();
*/

    final JobContext jContext = new JobContextImpl(conf,
        TASK_ATTEMPT_0.getJobID());
    AbstractS3GuardCommitter amCommitter = createCommitter(jContext);
    amCommitter.setupJob(jContext);

    final TaskAttemptContext[] taCtx = new TaskAttemptContextImpl[2];
    taCtx[0] = new TaskAttemptContextImpl(conf, TASK_ATTEMPT_0);
    taCtx[1] = new TaskAttemptContextImpl(conf, TASK_ATTEMPT_1);

    final TextOutputFormat[] tof = new LoggingTextOutputFormat[2];
    for (int i = 0; i < tof.length; i++) {
      tof[i] = new LoggingTextOutputFormat() {
        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context,
            String extension) throws IOException {
          final AbstractS3GuardCommitter foc = (AbstractS3GuardCommitter)
              getOutputCommitter(context);
          return new Path(new Path(foc.getWorkPath(), SUB_DIR),
              getUniqueFile(context, getOutputName(context), extension));
        }
      };
    }

    final ExecutorService executor = HadoopExecutors.newFixedThreadPool(2);
    try {
      for (int i = 0; i < taCtx.length; i++) {
        final int taskIdx = i;
        executor.submit(new Callable<Void>() {
          @Override
          public Void call() throws IOException, InterruptedException {
            final OutputCommitter outputCommitter =
                tof[taskIdx].getOutputCommitter(taCtx[taskIdx]);
            outputCommitter.setupTask(taCtx[taskIdx]);
            final RecordWriter rw =
                tof[taskIdx].getRecordWriter(taCtx[taskIdx]);
            writeOutput(rw, taCtx[taskIdx]);
            describe("Committing Task %d", taskIdx);
            outputCommitter.commitTask(taCtx[taskIdx]);
            return null;
          }
        });
      }
    } finally {
      executor.shutdown();
      while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
        LOG.info("Awaiting thread termination!");
      }
    }

    // if we commit here then all tasks will be committed, so there will
    // be contention for that final directory: both parts will go in.

    describe("\nCommitting Job");
    amCommitter.commitJob(jContext);
    assertPathExists("base output directory", outDir);
    assertPathDoesNotExist("Output ended up in base directory",
        new Path(outDir, PART_00000));
    Path outSubDir = new Path(outDir, SUB_DIR);
    assertPathDoesNotExist("Must not end up with sub_dir/sub_dir",
        new Path(outSubDir, SUB_DIR));

    // validate output
    // TODO: why false here?
    validateContent(outSubDir, false);
  }

  public String slurp(Path f) throws IOException {
    return ContractTestUtils.readUTF8(getFileSystem(), f, -1);
  }

  /**
   * Helper class for the failure simulation.
   * The first time {@link #exec()}
   * is called it will thrown an exception containing the string
   * {@link #COMMIT_FAILURE_MESSAGE}. The second time it will succeed.
   */
  public static class FailThenSucceed {
    private final AtomicBoolean firstTimeFail = new AtomicBoolean(true);

    public void exec() throws IOException {
      if (firstTimeFail.getAndSet(false)) {
        throw new IOException(COMMIT_FAILURE_MESSAGE);
      }
    }
  }
}
