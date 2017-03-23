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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.fs.contract.ContractTestUtils.listChildren;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.test.LambdaTestUtils.*;

/**
 * Test the job/task commit actions of an S3A Committer, including trying to
 * simulate some failure and retry conditions.
 * Derived from
 * {@code org.apache.hadoop.mapreduce.lib.output.TestFileOutputCommitter}.
 */
@SuppressWarnings({"unchecked", "ThrowableNotThrown"})
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

  private static final Text KEY_1 = new Text("key1");
  private static final Text KEY_2 = new Text("key2");
  private static final Text VAL_1 = new Text("val1");
  private static final Text VAL_2 = new Text("val2");

  /** A job to abort in test case teardown. */
  private JobData abortInTeardown;

  private void cleanupDestDir() throws IOException {
    rmdir(this.outDir, getConfiguration());
  }

  public void rmdir(Path dir, Configuration conf) throws IOException {
    describe("deleting %s", dir);
    FileSystem fs = dir.getFileSystem(conf);
    fs.delete(dir, true);
  }

  /**
   * This must return the name of a suite which is unique to the non-abstract
   * test.
   * @return a string which must be unique and a valid path.
   */
  protected abstract String suitename();

  @Override
  protected String getMethodName() {
    return suitename() + "-" + super.getMethodName();
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    outDir = path(getMethodName());
    S3AFileSystem fileSystem = getFileSystem();
    bindFileSystem(fileSystem, outDir, fileSystem.getConf());
    abortMultipartUploadsUnderPath(outDir);
    cleanupDestDir();
  }

  @Override
  public void teardown() throws Exception {
    describe("teardown");
    if (abortInTeardown != null) {
      abortQuietly(abortInTeardown);
    }
    abortMultipartUploadsUnderPath(outDir);
    cleanupDestDir();
    super.teardown();
  }

  protected void abortInTeardown(JobData jobData) {
    abortInTeardown = jobData;
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

  /**
   * Lambda Interface for creating committers, designed to allow
   * different factories to be used to create different failure modes.
   */
  public interface CommitterFactory {

    /**
     * Create a committer for a task.
     * @param context task context
     * @return new committer
     * @throws IOException failure
     */
    AbstractS3GuardCommitter createCommitter(
        TaskAttemptContext context) throws IOException;
  }

  /**
   * The normal committer creation factory, uses the abstract methods
   * in the class.
   */
  public class StandardCommitterFactory implements CommitterFactory {
    @Override
    public AbstractS3GuardCommitter createCommitter(TaskAttemptContext context)
        throws IOException {
      return AbstractITCommitProtocol.this.createCommitter(context);
    }
  }

  /**
   * Write some text out.
   * @param context task
   * @throws IOException IO failure
   * @throws InterruptedException write interrupted
   */
  protected void writeTextOutput(TaskAttemptContext context)
      throws IOException, InterruptedException {
    describe("write output");
    try (DurationInfo d = new DurationInfo("Writing Text output for task %s",
        context.getTaskAttemptID())) {
      writeOutput(new LoggingTextOutputFormat().getRecordWriter(context),
          context);
    }
  }

  /**
   * Write the standard output.
   * @param writer record write
   * @param context task context
   * @throws IOException IO failure
   * @throws InterruptedException write interrupted
   */
  private void writeOutput(RecordWriter theRecordWriter,
      TaskAttemptContext context) throws IOException, InterruptedException {
    NullWritable nullWritable = NullWritable.get();
    try {
      theRecordWriter.write(KEY_1, VAL_1);
      theRecordWriter.write(null, nullWritable);
      theRecordWriter.write(null, VAL_1);
      theRecordWriter.write(nullWritable, VAL_2);
      theRecordWriter.write(KEY_2, nullWritable);
      theRecordWriter.write(KEY_1, null);
      theRecordWriter.write(null, null);
      theRecordWriter.write(KEY_2, VAL_2);
    } finally {
      theRecordWriter.close(context);
    }
  }

  /**
   * Write the output.
   * @param writer record write
   * @param context task context
   * @param key key to write
   * @param val val to write
   * @throws IOException IO failure
   * @throws InterruptedException write interrupted
   */
  private void writeOutput(RecordWriter writer,
      TaskAttemptContext context, Text key, Text val)
      throws IOException, InterruptedException {
    NullWritable nullWritable = NullWritable.get();
    try {
      writer.write(key, val);
    } finally {
      writer.close(context);
    }
  }

  /**
   * Write the output of a map.
   * @param theRecordWriter
   * @param context
   * @throws IOException
   * @throws InterruptedException
   */
  private void writeMapFileOutput(RecordWriter theRecordWriter,
      TaskAttemptContext context) throws IOException, InterruptedException {
    describe("\nWrite map output");
    try (DurationInfo d = new DurationInfo("Writing Text output for task %s",
        context.getTaskAttemptID())) {
      for (int i = 0; i < 10; ++i) {
        Text val = ((i & 1) == 1) ? VAL_1 : VAL_2;
        theRecordWriter.write(new LongWritable(i), val);
      }
    } finally {
      theRecordWriter.close(context);
    }
  }

  /**
   * Details on a job for use in {@code startJob} and elsewhere.
   */
   public static class JobData {
     public final Job job;
     public final JobContext jContext;
     public final TaskAttemptContext tContext;
     public final AbstractS3GuardCommitter committer;
     public final Configuration conf;

     public JobData(Job job,
         JobContext jContext,
         TaskAttemptContext tContext,
         AbstractS3GuardCommitter committer) {
       this.job = job;
       this.jContext = jContext;
       this.tContext = tContext;
       this.committer = committer;
       conf = job.getConfiguration();
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

  /**
   * Start a job with a committer; optionally write the test data.
   * Always register the job to be aborted (quietly) in teardown.
   * This is, from an "OO-purity perspective" the wrong kind of method to
   * do: it's setting things up, mixing functionality, registering for teardown.
   * Its aim is simple though: a common body of code for starting work
   * in test cases.
   * @param writeText should the text be written?
   * @return the job data 4-tuple
   * @throws IOException IO problems
   * @throws InterruptedException interruption during write
   */
   protected JobData startJob(boolean writeText)
       throws IOException, InterruptedException {
     return startJob(new StandardCommitterFactory(), writeText);
   }

  /**
   * Start a job with a committer; optionally write the test data.
   * Always register the job to be aborted (quietly) in teardown.
   * This is, from an "OO-purity perspective" the wrong kind of method to
   * do: it's setting things up, mixing functionality, registering for teardown.
   * Its aim is simple though: a common body of code for starting work
   * in test cases.
   * @param factory the committer factory to use
   * @param writeText should the text be written?
   * @return the job data 4-tuple
   * @throws IOException IO problems
   * @throws InterruptedException interruption during write
   */
   protected JobData startJob(CommitterFactory factory, boolean writeText)
       throws IOException, InterruptedException {
     Job job = newJob();
     Configuration conf = job.getConfiguration();
     conf.set(MRJobConfig.TASK_ATTEMPT_ID, ATTEMPT_0);
     conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
     JobContext jContext = new JobContextImpl(conf, TASK_ATTEMPT_0.getJobID());
     TaskAttemptContext tContext = new TaskAttemptContextImpl(conf,
         TASK_ATTEMPT_0);
     AbstractS3GuardCommitter committer = factory.createCommitter(tContext);

     // setup
     setup(committer, jContext, tContext);
     JobData jobData = new JobData(job, jContext, tContext, committer);
     abortInTeardown(jobData);

     if (writeText) {
       // write output
       writeTextOutput(tContext);
     }
     return jobData;
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

  /**
   * Abort a job quietly.
   * @param jobData job info
   */
  protected void abortQuietly(JobData jobData) {
    abortQuietly(jobData.committer, jobData.jContext, jobData.tContext);
  }

  /**
   * Abort a job quietly: first task, then job.
   * @param committer committer
   * @param jContext job context
   * @param tContext task context
   */
  protected void abortQuietly(AbstractS3GuardCommitter committer,
      JobContext jContext,
      TaskAttemptContext tContext) {
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
   * Commit up the task and then the job.
   * @param committer committer
   * @param jContext job context
   * @param tContext task context
   * @throws IOException problems
   */
  protected void commit(AbstractS3GuardCommitter committer,
      JobContext jContext,
      TaskAttemptContext tContext) throws IOException {
    try (DurationInfo d = new DurationInfo("committing work",
        jContext.getJobID())) {
      describe("\ncommitting task");
      committer.commitTask(tContext);
      describe("\ncommitting job");
      committer.commitJob(jContext);
      describe("commit complete\n");
    }
  }

  /**
   * Execute work as part of a test, after creating the job.
   * After the execution, {@link #abortQuietly(JobData)} is
   * called for abort/cleanup.
   * @param name name of work (for logging)
   * @param action action to execute
   * @throws Exception failure
   */
  protected void executeWork(String name, ActionToTest action) throws Exception {
    JobData jobData = startJob(false);
    executeWork(name, jobData, action);
  }

  /**
   * Execute work as part of a test, against the created job.
   * After the execution, {@link #abortQuietly(JobData)} is
   * called for abort/cleanup.
   * @param name name of work (for logging)
   * @param jobData job info
   * @param action action to execute
   * @throws Exception failure
   */
  public void executeWork(String name,
      JobData jobData,
      ActionToTest action) throws Exception {
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3GuardCommitter committer = jobData.committer;
    try (DurationInfo d =
             new DurationInfo("Executing %s", name)) {
      action.exec(jobData.job, jContext, tContext, committer);
    } finally {
      abortQuietly(jobData);
    }
  }

  /**
   * Verify that recovery doesn't work for these committers.
   * @throws Exception
   */
  @Test
  public void testRecoveryAndCleanup() throws Exception {
    describe("Test (unsupported) task recovery.");
    JobData jobData = startJob(true);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3GuardCommitter committer = jobData.committer;

    // do commit
    committer.commitTask(tContext);
    Path attemptPath = committer.getTaskAttemptPath(tContext);
    assertPathDoesNotExist("commit dir", attemptPath);

    Configuration conf2 = jobData.job.getConfiguration();
    conf2.set(MRJobConfig.TASK_ATTEMPT_ID, ATTEMPT_0);
    conf2.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 2);
    JobContext jContext2 = new JobContextImpl(conf2, TASK_ATTEMPT_0.getJobID());
    TaskAttemptContext tContext2 = new TaskAttemptContextImpl(conf2,
        TASK_ATTEMPT_0);
    AbstractS3GuardCommitter committer2 = createCommitter(tContext2);
    committer2.setupJob(tContext2);

    assertFalse("recoverySupported in " + committer2,
        committer2.isRecoverySupported());
    intercept(PathCommitException.class, "recover",
        () -> committer2.recoverTask(tContext2));

    // at this point, task attempt 0 has failed to recover
    // it should be abortable though.
    committer2.abortTask(tContext2);
    committer2.abortJob(jContext2, JobStatus.State.KILLED);
    // now, state of system may still have pending data
    assertNoMultipartUploadsPending(outDir);
  }

  /**
   * Verify the output of the directory.
   * That includes the {@code part-m-00000-*}
   * file existence and contents, as well as optionally, the success marker.
   * @param dir directory to scan.
   * @param expectSuccessMarker check the success marker?
   * @throws IOException
   */
  private void validateContent(Path dir, boolean expectSuccessMarker)
      throws IOException {
    if (expectSuccessMarker) {
      assertSuccessMarkerExists(dir);
    }
    Path expectedFile = getPart0000(dir);
    LOG.debug("Validating content in {}", expectedFile);
    StringBuffer expectedOutput = new StringBuffer();
    expectedOutput.append(KEY_1).append('\t').append(VAL_1).append("\n");
    expectedOutput.append(VAL_1).append("\n");
    expectedOutput.append(VAL_2).append("\n");
    expectedOutput.append(KEY_2).append("\n");
    expectedOutput.append(KEY_1).append("\n");
    expectedOutput.append(KEY_2).append('\t').append(VAL_2).append("\n");
    String output = slurp(expectedFile);
    assertEquals("Content of " + expectedFile,
        expectedOutput.toString(), output);
  }

  /**
   * Identify any path under the directory which begins with the
   * {@code "part-m-00000"} sequence.
   * @param dir directory to scan
   * @return the full path
   * @throws FileNotFoundException the path is missing.
   * @throws IOException failure.
   */
  protected Path getPart0000(Path dir) throws IOException {
    FileStatus[] statuses = getFileSystem().listStatus(dir,
        new PathFilter() {
          @Override
          public boolean accept(Path path) {
            return path.getName().startsWith(PART_00000);
          }
        });
    if (statuses.length != 1) {
      // fail, with a listing of the parent dir
      assertPathExists("Output file", new Path(dir, PART_00000));
    }
    return statuses[0].getPath();
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
    Path expectedMapDir = getPart0000(dir);
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
   * Dump all MPUs in the filesystem.
   * @throws IOException IO failure
   */
  protected void dumpMultipartUploads() throws IOException {
    countMultipartUploads("");
  }

  /**
   * Assert that there *are* pending MPUs.
   * @param path path to look under
   * @throws IOException IO failure
   */
  protected void assertMultipartUploadsPending(Path path) throws IOException {
    int count = countMultipartUploads(path);
    assertTrue("No multipart uploads in progress under " + path, count > 0);
  }

  /**
   * Assert that there *are no* pending MPUs; assertion failure will include
   * the list of pending writes.
   * @param path path to look under
   * @throws IOException IO failure
   */
  protected void assertNoMultipartUploadsPending(Path path) throws IOException {
    List<String> uploads = listMultipartUploads(pathToPrefix(path));
    if (!uploads.isEmpty()) {
      StringBuilder result = new StringBuilder();
      for (String upload : uploads) {
        result.append(upload).append("; ");
      }
      fail("Multipart uploads in progress under " + path + " \n" + result);
    }
  }

  /**
   * Count the number of MPUs under a path
   * @param path path to scan
   * @return count
   * @throws IOException IO failure
   */
  protected int countMultipartUploads(Path path) throws IOException {
    return countMultipartUploads(pathToPrefix(path));
  }

  /**
   * Count the number of MPUs under a prefix
   * @param path path to scan
   * @return count
   * @throws IOException IO failure
   */
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
   * Map from a path to a prefix
   * @param path path
   * @return the key
   */
  private String pathToPrefix(Path path) {
    return path == null ? "" :
        getFileSystem().pathToKey(path);
  }

  /**
   * Get a list of all pending uploads under a prefix, one which can be printed.
   * @param prefix prefix to look under
   * @return possibly empty list
   * @throws IOException IO failure.
   */
  protected List<String> listMultipartUploads(String prefix) throws IOException {
    List<MultipartUpload> uploads = getFileSystem().listMultipartUploads(
        prefix);
    List<String> result = new ArrayList<>(uploads.size());

    for (MultipartUpload upload : uploads) {
      result.add(String.format("Upload to %s with ID %s",
          upload.getKey(), upload.getUploadId()));
    }
    return result;
  }

  /**
   * Abort all multipart uploads under a path.
   * @param path path for uploads to abort
   * @return a count of aborts
   * @throws IOException trouble.
   */
  protected int abortMultipartUploadsUnderPath(Path path) throws IOException {
    S3AFileSystem fs = getFileSystem();
    String key = fs.pathToKey(path);
    S3AFileSystem.WriteOperationHelper writeOps
        = fs.createWriteOperationHelper(key);
    int count = writeOps.abortMultipartUploadsUnderPath(key);
    if (count > 0) {
      LOG.info("Multipart uploads deleted: {}", count);
    }
    return count;
  }

  /**
   * Full test of the expected lifecycle: start job, task, write, commit task,
   * commit job.
   * @throws Exception on a failure
   */
  @Test
  public void testCommitLifecycle() throws Exception {
    describe("Full test of the expected lifecycle:\n" +
        " start job, task, write, commit task, commit job.\n" +
        "Verify:\n" +
        "* no files are visible after task commit\n" +
        "* the expected file is visible after job commit\n" +
        "* no outstanding MPUs after job commit");
    JobData jobData = startJob(false);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3GuardCommitter committer = jobData.committer;

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
      iterateOverFiles(files,
          (LocatedFileStatus status) -> {
            assertFalse("task committed file to dest :" + status,
                status.getPath().toString().contains("part"));
          });
    } catch (FileNotFoundException e) {
      LOG.info("Outdir {} is not created by task commit phase ",
          outDir);
    }

    describe("3. Committing job");
    assertMultipartUploadsPending(outDir);
    committer.commitJob(jContext);

    // validate output
    describe("4. Validating content");
    validateContent(outDir, shouldExpectSuccessMarker());
    assertNoMultipartUploadsPending(outDir);

  }

  @Test
  public void testCommitterWithDuplicatedCommit() throws Exception {
    describe("Call a task then job commit twice;" +
        "expect the second task commit to fail.");
    JobData jobData = startJob(true);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3GuardCommitter committer = jobData.committer;

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
    JobData jobData = startJob(new FailingCommitterFactory(), true);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3GuardCommitter committer = jobData.committer;

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
   * Create a committer which fails; the class {@link FaultInjection}
   * implements the logic.
   * @param tContext task context
   * @return committer instance
   * @throws IOException failure to instantiate
   */
  protected abstract AbstractS3GuardCommitter createFailingCommitter(
      TaskAttemptContext tContext) throws IOException;


  /**
   * Factory for failing committers.
   */
  public class FailingCommitterFactory implements CommitterFactory {
    @Override
    public AbstractS3GuardCommitter createCommitter(TaskAttemptContext context)
        throws IOException {
      return createFailingCommitter(context);
    }
  }

  protected void expectFNFEonTaskCommit(AbstractS3GuardCommitter committer,
      TaskAttemptContext tContext) throws Exception {
    intercept(FileNotFoundException.class,
        () -> committer.commitTask(tContext));
  }

  /**
   * Assert that the output dir has the {@code _SUCCESS} marker.
   * @throws IOException
   */
  protected void assertSuccessMarkerExists() throws IOException {
    assertSuccessMarkerExists(outDir);
  }

  /**
   * Assert that the specified dir has the {@code _SUCCESS} marker.
   * @param dir dir to scan
   * @throws IOException IO Failure
   */
  protected void assertSuccessMarkerExists(Path dir) throws IOException {
    assertPathExists("Success marker",
        new Path(dir, SUCCESS_FILE_NAME));
  }

  /**
   * Assert that the given dir does not have the {@code _SUCCESS} marker.
   * @param dir dir to scan
   * @throws IOException IO Failure
   */
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
    JobData jobData = startJob(new FailingCommitterFactory(), false);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3GuardCommitter committer = jobData.committer;

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
    describe("Test that the committer generates map output into a directory\n" +
        "starting with the prefix part-0000");
    JobData jobData = startJob(false);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3GuardCommitter committer = jobData.committer;
    Configuration conf = jobData.conf;

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
        fileStatus.getPath().getName().startsWith(PART_00000));

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
        return !(name.startsWith("_") || name.startsWith("."));
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

  public interface ActionToTest {
    void exec(Job job, JobContext jContext, TaskAttemptContext tContext,
        AbstractS3GuardCommitter committer) throws Exception;
  }

  @Test
  public void testAbortTaskNoWorkDone() throws Exception {
    executeWork("abort task no work",
        ((job, jContext, tContext, committer) ->
             committer.abortTask(tContext)));
  }

  @Test
  public void testAbortJobNoWorkDone() throws Exception {
    executeWork("abort task no work",
        ((job, jContext, tContext, committer) -> {
          committer.abortJob(jContext, JobStatus.State.RUNNING);
        }));
  }

  @Test
  public void testCommitJobButNotTask() throws Exception {
    executeWork("commit a job while a task works is pending, " +
            "expect task writes to be cancelled.",
        ((job, jContext, tContext, committer) -> {
          // step 1: write the text
          writeTextOutput(tContext);
          // step 2: commit the job
          AbstractS3GuardCommitter jobCommitter = createCommitter(jContext);
          jobCommitter.commitJob(jContext);
          assertPart0000DoesNotExist(outDir);
          assertNoMultipartUploadsPending(outDir);
        }));
  }

  @Test
  public void testAbortTaskThenJob() throws Exception {
    JobData jobData = startJob(true);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3GuardCommitter committer = jobData.committer;
    Configuration conf = jobData.conf;

    // do abort
    committer.abortTask(tContext);

    intercept(FileNotFoundException.class, "",
        () -> getPart0000(committer.getWorkPath()));

    committer.abortJob(jContext, JobStatus.State.FAILED);
    assertJobAbortCleanedUp(jobData);
  }

  /**
   * Extension point: assert that the job was all cleaned up after an abort.
   * Base assertions
   * <ul>
   *   <li>Output dir is absent or, if present, empty</li>
   *   <li>No pending MPUs to/under the output dir</li>
   * </ul>
   * @param jobData job data
   * @throws Exception failure
   */
  public void assertJobAbortCleanedUp(JobData jobData) throws Exception {
    // special handling of magic directory; harmless in staging
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
    assertNoMultipartUploadsPending(outDir);
  }

  @Test
  public void testFailAbort() throws Exception {
    describe("Abort the task, then job (failed), abort the job again");
    JobData jobData = startJob(true);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3GuardCommitter committer = jobData.committer;
    Configuration conf = jobData.conf;

    // do abort
    committer.abortTask(tContext);

    Path jtd = committer.getJobAttemptPath(jContext);
    Path ttd = committer.getTaskAttemptPath(tContext);
    assertPart0000DoesNotExist(outDir);
    assertSuccessMarkerDoesNotExist(outDir);
    describe("Aborting job into %s", outDir);

    committer.abortJob(jContext, JobStatus.State.FAILED);

    assertPathDoesNotExist("job temp dir", jtd);

    // try again; expect abort to be idempotent.
    committer.abortJob(jContext, JobStatus.State.FAILED);
    assertNoMultipartUploadsPending(outDir);
  }

  public void assertPart0000DoesNotExist(Path dir) throws IOException {
    try {
      Path p = getPart0000(dir);
      // bad
      fail("Unexpectedly found generated part-0000 output file "  + p);
    } catch (FileNotFoundException e) {
      // good
    }
    assertPathDoesNotExist("expected output file", new Path(dir, PART_00000));
  }

  @Test
  public void testAbortJobNotTask() throws Exception {
    executeWork("abort task no work",
        ((job, jContext, tContext, committer) -> {
          // write output
          writeTextOutput(tContext);
          committer.abortJob(jContext, JobStatus.State.RUNNING);
          ContractTestUtils.assertPathDoesNotExist(
              committer.getTaskAttemptFilesystem(tContext),
              "Task attempt local working dir",
              committer.getTaskAttemptPath(tContext));
          assertPathDoesNotExist("job temp dir",
              committer.getJobAttemptPath(jContext));
          assertNoMultipartUploadsPending(outDir);
        }));
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
    assertPart0000DoesNotExist(outDir);
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
   * The first time {@link #commitJob()}
   * is called it will thrown an exception containing the string
   * {@link #COMMIT_FAILURE_MESSAGE}. The second time it will succeed.
   */
  public static class FaultInjection {
    private final AtomicBoolean firstTimeFail = new AtomicBoolean(true);

    public void commitJob() throws IOException {
      if (firstTimeFail.getAndSet(false)) {
        throw new IOException(COMMIT_FAILURE_MESSAGE);
      }
    }
  }
}
