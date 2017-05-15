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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.Mockito.*;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingTestBase.*;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;

/** Mocking test of partitioned committer. */
public class TestStagingPartitionedJobCommit
    extends StagingTestBase.JobCommitterTest<PartitionedStagingCommitter> {

  @Override
  public void setupJob() throws Exception {
    super.setupJob();
    getWrapperFS().setLogEvents(MockS3AFileSystem.LOG_NAME);
  }

  @Override
  PartitionedStagingCommitter newJobCommitter() throws IOException {
    return new PartitionedStagingCommitterForTesting(getJob());
  }

  /**
   * Subclass of the Partitioned Staging committer used in the test cases.
   */
  private static final class PartitionedStagingCommitterForTesting
      extends PartitionedCommitterForTesting {

    private boolean aborted = false;

    private PartitionedStagingCommitterForTesting(JobContext context)
        throws IOException {
      super(OUTPUT_PATH, context);
    }

    @Override
    protected List<SinglePendingCommit> getPendingUploadsToCommit(
        JobContext context) throws IOException {
      List<SinglePendingCommit> pending = Lists.newArrayList();

      for (String dateint : Arrays.asList("20161115", "20161116")) {
        for (String hour : Arrays.asList("13", "14")) {
          String key = OUTPUT_PREFIX + "/dateint=" + dateint + "/hour=" + hour +
              "/" + UUID.randomUUID().toString() + ".parquet";
          SinglePendingCommit commit = new SinglePendingCommit();
          commit.bucket = BUCKET;
          commit.destinationKey = key;
          commit.uri = "s3a://" + BUCKET + "/" + key;
          commit.uploadId = UUID.randomUUID().toString();
          commit.etags = new ArrayList<>();
          pending.add(commit);
        }
      }
      return pending;
    }

    @Override
    protected void abortJobInternal(JobContext context,
        List<SinglePendingCommit> pending,
        boolean suppressExceptions) throws IOException {
      this.aborted = true;
      super.abortJobInternal(context, pending, suppressExceptions);
    }
  }

  @Test
  public void testDefaultFailAndAppend() throws Exception {
    FileSystem mockS3 = getMockS3A();

    // both fail and append don't check. fail is enforced at the task level.
    for (String mode : Arrays.asList(null, CONFLICT_MODE_FAIL,
        CONFLICT_MODE_APPEND)) {
      if (mode != null) {
        getJob().getConfiguration().set(
            FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, mode);
      } else {
        getJob().getConfiguration().unset(
            FS_S3A_COMMITTER_STAGING_CONFLICT_MODE);
      }

      PartitionedStagingCommitter committer = newJobCommitter();

      // no directories exist
      committer.commitJob(getJob());

      // parent and peer directories exist
      reset(mockS3);
      pathsExist(mockS3, "dateint=20161116",
          "dateint=20161116/hour=10");
      committer.commitJob(getJob());
      verifyCompletion(mockS3);

      // a leaf directory exists.
      // NOTE: this is not checked during job commit, the commit succeeds.
      reset(mockS3);
      pathsExist(mockS3, "dateint=20161115/hour=14");
      committer.commitJob(getJob());
      verifyCompletion(mockS3);
    }
  }

  @Test
  public void testBadConflictMode() throws Throwable {
    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, "merge");
    assertThrows("commiter conflict", IllegalArgumentException.class,
        "MERGE", this::newJobCommitter);
  }

  @Test
  public void testReplace() throws Exception {
    S3AFileSystem mockS3 = getMockS3A();

    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_REPLACE);

    PartitionedStagingCommitter committer = newJobCommitter();

//    getWrapperFS().setLogEvents(MockS3AFileSystem.LOG_STACK);
    committer.commitJob(getJob());
    verifyReplaceCommitActions(mockS3);
    verifyCompletion(mockS3);

    // parent and peer directories exist
    reset(mockS3);
    pathsExist(mockS3, "dateint=20161115",
        "dateint=20161115/hour=12");

    committer.commitJob(getJob());
    verifyReplaceCommitActions(mockS3);
    verifyCompletion(mockS3);

    // partition directories exist and should be removed
    reset(mockS3);
    pathsExist(mockS3, "dateint=20161115/hour=12",
        "dateint=20161115/hour=13");
    canDelete(mockS3, "dateint=20161115/hour=13");

    committer.commitJob(getJob());
    verifyDeleted(mockS3, "dateint=20161115/hour=13");
    verifyReplaceCommitActions(mockS3);
    verifyCompletion(mockS3);

    // partition directories exist and should be removed
    reset(mockS3);
    pathsExist(mockS3, "dateint=20161116/hour=13",
        "dateint=20161116/hour=14");

    canDelete(mockS3, "dateint=20161116/hour=13",
        "dateint=20161116/hour=14");

    committer.commitJob(getJob());
    verifyReplaceCommitActions(mockS3);
    verifyDeleted(mockS3, "dateint=20161116/hour=13");
    verifyDeleted(mockS3, "dateint=20161116/hour=14");
    verifyCompletion(mockS3);
  }


  /**
   * Verify the actions which replace does, essentially: delete the parent
   * partitions.
   * @param mockS3 s3 mock
   */
  protected void verifyReplaceCommitActions(FileSystem mockS3)
      throws IOException {
    verifyDeleted(mockS3, "dateint=20161115/hour=13");
    verifyDeleted(mockS3, "dateint=20161115/hour=14");
    verifyDeleted(mockS3, "dateint=20161116/hour=13");
    verifyDeleted(mockS3, "dateint=20161116/hour=14");
  }

  /**
   * The exists() check before the delete has been cut as not needed; it
   * only adds 1-4 HTTP requests which the delete() call will need to do anyway.
   */
//  @Test
  public void testReplaceWithExistsFailure() throws Exception {
    FileSystem mockS3 = getMockS3A();

    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_REPLACE);

    final PartitionedStagingCommitter committer = newJobCommitter();

    pathsExist(mockS3, "dateint=20161115/hour=13");
    canDelete(mockS3, "dateint=20161115/hour=13");
    String message = "Fake IOException for delete";
    when(mockS3.exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=14")))
        .thenThrow(new IOException(message));

    StagingTestBase.assertThrows("Should throw the fake IOException",
        IOException.class, message,
        new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            committer.commitJob(getJob());
            return null;
          }
        });

    verifyExistenceChecked(mockS3, "dateint=20161115/hour=13");
    verifyDeleted(mockS3, "dateint=20161115/hour=13");
    verifyExistenceChecked(mockS3, "dateint=20161115/hour=14");
    assertTrue("Should have aborted",
        ((PartitionedStagingCommitterForTesting) committer).aborted);
    verifyCompletion(mockS3);
  }

  @Test
  public void testReplaceWithDeleteFailure() throws Exception {
    FileSystem mockS3 = getMockS3A();

    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_REPLACE);

    final PartitionedStagingCommitter committer = newJobCommitter();

    pathsExist(mockS3, "dateint=20161116/hour=14");
    when(mockS3
        .delete(
            new Path(OUTPUT_PATH, "dateint=20161116/hour=14"),
            true))
        .thenThrow(new IOException("Fake IOException for delete"));

    intercept(IOException.class, null, "Should throw the fake IOException",
        new LambdaTestUtils.VoidCallable() {
          @Override
          public void call() throws IOException {
            committer.commitJob(getJob());
          }
        });

    verifyReplaceCommitActions(mockS3);
    verifyDeleted(mockS3, "dateint=20161116/hour=14");
    assertTrue("Should have aborted",
        ((PartitionedStagingCommitterForTesting) committer).aborted);
    verifyCompletion(mockS3);
  }

}
