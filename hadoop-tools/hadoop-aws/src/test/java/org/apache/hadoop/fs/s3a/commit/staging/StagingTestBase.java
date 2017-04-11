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

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.MiniDFSTestCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.service.ServiceOperations;

import static org.apache.hadoop.test.LambdaTestUtils.VoidCallable;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Test base for staging: core constants and static methods, inner classes
 * for specific test types.
 */
public class StagingTestBase {

  public static final String BUCKET = MockS3AFileSystem.BUCKET;
  public static final String OUTPUT_PREFIX = "output/path";
  public static final Path OUTPUT_PATH = new Path(
      "s3a://" + BUCKET + "/" + OUTPUT_PREFIX);
  public static final URI OUTPUT_PATH_URI = OUTPUT_PATH.toUri();
  public static final URI FS_URI = URI.create("s3a://" + BUCKET + "/");
  private static final Logger LOG =
      LoggerFactory.getLogger(StagingTestBase.class);

  protected StagingTestBase() {
  }

  /**
   * Sets up the mock filesystem instance and binds it to the
   * {@link FileSystem#get(URI, Configuration)} call for the supplied URI
   * and config.
   * All standard mocking setup MUST go here.
   * @param conf config to use
   * @return the filesystem created
   * @throws IOException IO problems.
   */
  protected static S3AFileSystem createAndBindMockFSInstance(Configuration conf)
      throws IOException {
    S3AFileSystem mockFs = mock(S3AFileSystem.class);
    MockS3AFileSystem wrapperFS = new MockS3AFileSystem(mockFs);
    URI uri = OUTPUT_PATH_URI;
    wrapperFS.initialize(uri, conf);
    FileSystemTestHelper.addFileSystemForTesting(uri, conf, wrapperFS);
    return mockFs;
  }

  /**
   * Look up the FS by URI, return a (cast) Mock wrapper.
   * @param conf config
   * @return the FS
   * @throws IOException IO Failure
   */
  public static MockS3AFileSystem lookupWrapperFS(Configuration conf)
      throws IOException {
    return (MockS3AFileSystem) FileSystem.get(OUTPUT_PATH_URI, conf);
  }

  public static void verifyCompletion(FileSystem mockS3) throws IOException {
    verifyCleanupTempFiles(mockS3);
    verifyNoMoreInteractions(mockS3);
  }

  public static void verifyDeleted(FileSystem mockS3, Path path)
      throws IOException {
    verify(mockS3).delete(path, true);
  }

  public static void verifyDeleted(FileSystem mockS3, String child)
      throws IOException {
    verifyDeleted(mockS3, new Path(OUTPUT_PATH, child));
  }

  public static void verifyCleanupTempFiles(FileSystem mockS3)
      throws IOException {
    verifyDeleted(mockS3,
        new Path(OUTPUT_PATH, CommitConstants.PENDING_DIR_NAME));
  }

  protected static void assertConflictResolution(
      StagingS3GuardCommitter committer,
      JobContext job,
      ConflictResolution mode) {
    Assert.assertEquals("Conflict resolution mode in " + committer,
        mode, committer.getConflictResolutionMode(job));
  }

  public static void pathsExist(FileSystem mockS3, String... children)
      throws IOException {
    for (String child : children) {
      pathExists(mockS3, new Path(OUTPUT_PATH, child));
    }
  }

  public static void pathExists(FileSystem mockS3, Path path)
      throws IOException {
    when(mockS3.exists(path)).thenReturn(true);
  }

  public static void pathDoesNotExist(FileSystem mockS3, Path path)
      throws IOException {
    when(mockS3.exists(path)).thenReturn(false);
  }

  public static void canDelete(FileSystem mockS3, String... children)
      throws IOException {
    for (String child : children) {
      canDelete(mockS3, new Path(OUTPUT_PATH, child));
    }
  }

  public static void canDelete(FileSystem mockS3, Path f) throws IOException {
    when(mockS3.delete(f,
        true /* recursive */))
        .thenReturn(true);
  }

  public static void verifyExistenceChecked(FileSystem mockS3, String child)
      throws IOException {
    verifyExistenceChecked(mockS3, new Path(OUTPUT_PATH, child));
  }

  public static void verifyExistenceChecked(FileSystem mockS3, Path path)
      throws IOException {
    verify(mockS3).exists(path);
  }

  /**
   * Provides setup/teardown of a MiniDFSCluster for tests that need one.
   */
  public static class MiniDFSTest extends Assert {

    private static MiniDFSTestCluster hdfs;

    private static JobConf conf = null;

    protected static JobConf getConfiguration() {
      return conf;
    }

    protected static FileSystem getDFS() {
      return hdfs.getClusterFS();
    }

    @BeforeClass
    @SuppressWarnings("deprecation")
    public static void setupFS() throws IOException {
      if (hdfs == null) {
        JobConf c = new JobConf();
        hdfs = new MiniDFSTestCluster();
        hdfs.init(c);
        hdfs.start();
        conf = c;
      }
    }

    @AfterClass
    public static void teardownFS() throws IOException {
      conf = null;
      if (hdfs != null) {
        ServiceOperations.stopQuietly(hdfs);
        hdfs = null;
      }
    }

    public static JobConf getConf() {
      return conf;
    }

    public static void setConf(JobConf conf) {
      MiniDFSTest.conf = conf;
    }

    public static MiniDFSCluster getHdfs() {
      return hdfs.getCluster();
    }

    public static FileSystem getLocalFS() {
      return hdfs.getLocalFS();
    }

  }

  /**
   * Base class for job committer tests.
   * @param <C> committer
   */
  public abstract static class JobCommitterTest<C extends OutputCommitter>
      extends Assert {
    private static final JobID JOB_ID = new JobID("job", 1);
    private JobConf jobConf;

    // created in BeforeClass
    private S3AFileSystem mockFS = null;
    private MockS3AFileSystem wrapperFS = null;
    private JobContext job = null;

    // created in Before
    private StagingTestBase.ClientResults results = null;
    private StagingTestBase.ClientErrors errors = null;
    private AmazonS3 mockClient = null;

    @Before
    public void setupJob() throws Exception {
      this.jobConf = new JobConf();
      jobConf.set(CommitConstants.FS_S3A_COMMITTER_STAGING_UUID,
          UUID.randomUUID().toString());
      jobConf.setBoolean(
          CommitConstants.CREATE_SUCCESSFUL_JOB_OUTPUT_DIR_MARKER,
          false);

      this.mockFS = createAndBindMockFSInstance(jobConf);
      this.wrapperFS = lookupWrapperFS(jobConf);
      this.job = new JobContextImpl(jobConf, JOB_ID);
      this.results = new StagingTestBase.ClientResults();
      this.errors = new StagingTestBase.ClientErrors();
      this.mockClient = newMockClient(results, errors);
      // and bind the FS
      wrapperFS.setAmazonS3Client(mockClient);
    }

    public S3AFileSystem getMockS3() {
      return mockFS;
    }

    public MockS3AFileSystem getWrapperFS() {
      return wrapperFS;
    }

    public JobContext getJob() {
      return job;
    }

    protected StagingTestBase.ClientResults getMockResults() {
      return results;
    }

    protected StagingTestBase.ClientErrors getMockErrors() {
      return errors;
    }

    protected AmazonS3 getMockClient() {
      return mockClient;
    }

    abstract C newJobCommitter() throws Exception;
  }

  /** */
  public abstract static class TaskCommitterTest<C extends OutputCommitter>
      extends JobCommitterTest<C> {
    private static final TaskAttemptID AID = new TaskAttemptID(
        new TaskID(JobCommitterTest.JOB_ID, TaskType.REDUCE, 2), 3);

    private C jobCommitter = null;
    private TaskAttemptContext tac = null;

    @Before
    public void setupTask() throws Exception {
      this.jobCommitter = newJobCommitter();
      jobCommitter.setupJob(getJob());

      this.tac = new TaskAttemptContextImpl(
          new Configuration(getJob().getConfiguration()), AID);

      // get the task's configuration copy so modifications take effect
      // TODO: This is a different property than that used in the committer
      tac.getConfiguration().set("mapred.local.dir",
              System.getProperty(StagingCommitterConstants.JAVA_IO_TMPDIR));
    }

    protected C getJobCommitter() {
      return jobCommitter;
    }

    protected TaskAttemptContext getTAC() {
      return tac;
    }

    abstract C newTaskCommitter() throws Exception;
  }

  /**
   * Results accrued during mock runs.
   * This data is serialized in MR Tests and read back in in the test runner
   */
  public static class ClientResults implements Serializable {
    // For inspection of what the committer did
    public final Map<String, InitiateMultipartUploadRequest> requests =
        Maps.newHashMap();
    public final List<String> uploads = Lists.newArrayList();
    public final List<UploadPartRequest> parts = Lists.newArrayList();
    public final Map<String, List<String>> tagsByUpload = Maps.newHashMap();
    public final List<CompleteMultipartUploadRequest> commits =
        Lists.newArrayList();
    public final List<AbortMultipartUploadRequest> aborts
        = Lists.newArrayList();
    public final List<DeleteObjectRequest> deletes = Lists.newArrayList();

    public Map<String, InitiateMultipartUploadRequest> getRequests() {
      return requests;
    }

    public List<String> getUploads() {
      return uploads;
    }

    public List<UploadPartRequest> getParts() {
      return parts;
    }

    public Map<String, List<String>> getTagsByUpload() {
      return tagsByUpload;
    }

    public List<CompleteMultipartUploadRequest> getCommits() {
      return commits;
    }

    public List<AbortMultipartUploadRequest> getAborts() {
      return aborts;
    }

    public List<DeleteObjectRequest> getDeletes() {
      return deletes;
    }

    public void resetDeletes() { deletes.clear(); }

    public void resetUploads() {
      uploads.clear();
    }

    public void resetCommits() {
      commits.clear();
    }

    public void resetRequests() {
      requests.clear();
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          super.toString());
      sb.append("{ requests=").append(requests.size());
      sb.append(", uploads=").append(uploads.size());
      sb.append(", parts=").append(parts.size());
      sb.append(", tagsByUpload=").append(tagsByUpload.size());
      sb.append(", commits=").append(commits.size());
      sb.append(", aborts=").append(aborts.size());
      sb.append(", deletes=").append(deletes.size());
      sb.append('}');
      return sb.toString();
    }
  }

  /** Control errors to raise in mock S3 client. */
  public static class ClientErrors {
    // For injecting errors
    public int failOnInit = -1;
    public int failOnUpload = -1;
    public int failOnCommit = -1;
    public int failOnAbort = -1;
    public boolean recover = false;

    public void failOnInit(int initNum) {
      this.failOnInit = initNum;
    }

    public void failOnUpload(int uploadNum) {
      this.failOnUpload = uploadNum;
    }

    public void failOnCommit(int commitNum) {
      this.failOnCommit = commitNum;
    }

    public void failOnAbort(int abortNum) {
      this.failOnAbort = abortNum;
    }

    public void recoverAfterFailure() {
      this.recover = true;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "ClientErrors{");
      sb.append("failOnInit=").append(failOnInit);
      sb.append(", failOnUpload=").append(failOnUpload);
      sb.append(", failOnCommit=").append(failOnCommit);
      sb.append(", failOnAbort=").append(failOnAbort);
      sb.append(", recover=").append(recover);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Instantiate mock client with the results and errors requested.
   * @param results results to accrue
   * @param errors when (if any) to fail
   * @return the mock client to patch in to a committer/FS instance
   */
  public static AmazonS3 newMockClient(final ClientResults results,
      final ClientErrors errors) {
    AmazonS3Client mockClient = mock(AmazonS3Client.class);
    final Object lock = new Object();

    // initiateMultipartUpload
    when(mockClient
        .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
        .thenAnswer(new Answer<InitiateMultipartUploadResult>() {
          @Override
          public InitiateMultipartUploadResult answer(
              InvocationOnMock invocation) throws Throwable {
            LOG.debug("initiateMultipartUpload for {}", mockClient);
            synchronized (lock) {
              if (results.requests.size() == errors.failOnInit) {
                if (errors.recover) {
                  errors.failOnInit(-1);
                }
                throw new AmazonClientException(
                    "Fail on init " + results.requests.size());
              }
              String uploadId = UUID.randomUUID().toString();
              results.requests.put(uploadId, invocation.getArgumentAt(
                  0, InitiateMultipartUploadRequest.class));
              results.uploads.add(uploadId);
              return newResult(results.requests.get(uploadId), uploadId);
            }
          }
        });

    // uploadPart
    when(mockClient.uploadPart(any(UploadPartRequest.class)))
        .thenAnswer(new Answer<UploadPartResult>() {
          @Override
          public UploadPartResult answer(InvocationOnMock invocation)
              throws Throwable {
            LOG.debug("uploadPart for {}", mockClient);
            synchronized (lock) {
              if (results.parts.size() == errors.failOnUpload) {
                if (errors.recover) {
                  errors.failOnUpload(-1);
                }
                LOG.info("Triggering upload failure");
                throw new AmazonClientException(
                    "Fail on upload " + results.parts.size());
              }
              UploadPartRequest req = invocation.getArgumentAt(
                  0, UploadPartRequest.class);
              results.parts.add(req);
              String etag = UUID.randomUUID().toString();
              List<String> etags = results.tagsByUpload.get(req.getUploadId());
              if (etags == null) {
                etags = Lists.newArrayList();
                results.tagsByUpload.put(req.getUploadId(), etags);
              }
              etags.add(etag);
              return newResult(req, etag);
            }
          }
        });

    // completeMultipartUpload
    when(mockClient
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
        .thenAnswer(new Answer<CompleteMultipartUploadResult>() {
          @Override
          public CompleteMultipartUploadResult answer(
              InvocationOnMock invocation) throws Throwable {
            LOG.debug("completeMultipartUpload for {}", mockClient);
            synchronized (lock) {
              if (results.commits.size() == errors.failOnCommit) {
                if (errors.recover) {
                  errors.failOnCommit(-1);
                }
                throw new AmazonClientException(
                    "Fail on commit " + results.commits.size());
              }
              CompleteMultipartUploadRequest req = invocation.getArgumentAt(
                  0, CompleteMultipartUploadRequest.class);
              results.commits.add(req);
              return newResult(req);
            }
          }
        });

    // abortMultipartUpload mocking
    doAnswer(
        new Answer<Void>() {
          @Override
          public Void answer(
              InvocationOnMock invocation) throws Throwable {
            LOG.debug("abortMultipartUpload for {}", mockClient);
            synchronized (lock) {
              if (results.aborts.size() == errors.failOnAbort) {
                if (errors.recover) {
                  errors.failOnAbort(-1);
                }
                throw new AmazonClientException(
                    "Fail on abort " + results.aborts.size());
              }
              results.aborts.add(invocation.getArgumentAt(
                  0, AbortMultipartUploadRequest.class));
              return null;
            }
          }
        })
        .when(mockClient)
        .abortMultipartUpload(any(AbortMultipartUploadRequest.class));

    // deleteObject mocking
    doAnswer(new Answer<Void>() {
          @Override
          public Void answer(
              InvocationOnMock invocation) throws Throwable {
            LOG.debug("deleteObject for {}", mockClient);
            synchronized (lock) {
              results.deletes.add(invocation.getArgumentAt(
                  0, DeleteObjectRequest.class));
              return null;
            }
          }
        })
        .when(mockClient)
        .deleteObject(any(DeleteObjectRequest.class));

    // to String returns the debug information
    when(mockClient.toString()).thenAnswer(
        new Answer<String>() {
          @Override
          public String answer(InvocationOnMock invocation) throws Throwable {
            return "Mock3AClient " + results + " " + errors;
          }
        });


    return mockClient;
  }

  private static CompleteMultipartUploadResult newResult(
      CompleteMultipartUploadRequest req) {
    return new CompleteMultipartUploadResult();
  }

  private static UploadPartResult newResult(UploadPartRequest request,
      String etag) {
    UploadPartResult result = new UploadPartResult();
    result.setPartNumber(request.getPartNumber());
    result.setETag(etag);
    return result;
  }

  private static InitiateMultipartUploadResult newResult(
      InitiateMultipartUploadRequest request, String uploadId) {
    InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
    result.setUploadId(uploadId);
    return result;
  }

  /**
   * create files in the attempt path that should be found by
   * {@code getTaskOutput}.
   * @param relativeFiles list of files relative to address path
   * @param attemptPath
   * @param conf config for FS
   * @throws IOException on any failure
   */
  public static void createTestOutputFiles(List<String> relativeFiles,
      Path attemptPath,
      Configuration conf) throws IOException {
    //
    FileSystem attemptFS = attemptPath.getFileSystem(conf);
    attemptFS.delete(attemptPath, true);
    for (String relative : relativeFiles) {
      // 0-length files are ignored, so write at least one byte
      OutputStream out = attemptFS.create(new Path(attemptPath, relative));
      out.write(34);
      out.close();
    }
  }

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests.
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param callable A Callable that is expected to throw the exception
   */
  public static void assertThrows(
      String message, Class<? extends Exception> expected, Callable<?> callable)
      throws Exception {
    assertThrows(message, expected, null, callable);
  }

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests.
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param callable A Callable that is expected to throw the exception
   */
  public static void assertThrows(
      String message, Class<? extends Exception> expected, String expectedMsg,
      Callable<?> callable) throws Exception {
    intercept(expected, expectedMsg, message, callable);
  }

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests.
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param runnable A Runnable that is expected to throw the exception
   */
  public static void assertThrows(
      String message, Class<? extends Exception> expected, Runnable runnable)
      throws Exception {
    assertThrows(message, expected, null, runnable);
  }

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests.
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param runnable A Runnable that is expected to throw the exception
   */
  public static void assertThrows(
      String message, Class<? extends Exception> expected, String expectedMsg,
      Runnable runnable) throws Exception {
    intercept(expected, expectedMsg, message,
        new VoidCallable() {
          @Override
          public void call() throws Exception {
            runnable.run();
          }
        });
  }
}
