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

package org.apache.hadoop.fs.s3a.commit.staging.integration;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.AbstractITCommitProtocol;
import org.apache.hadoop.fs.s3a.commit.AbstractS3GuardCommitter;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.FaultInjection;
import org.apache.hadoop.fs.s3a.commit.FaultInjectionImpl;
import org.apache.hadoop.fs.s3a.commit.staging.Paths;
import org.apache.hadoop.fs.s3a.commit.staging.StagingS3GuardCommitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.DIRECTORY_COMMITTER_FACTORY;

/** Test the staging committer's handling of the base protocol operations. */
public class ITestStagingCommitProtocol extends AbstractITCommitProtocol {

  @Override
  protected String suitename() {
    return "ITestStagingCommitProtocol";
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setInt(CommitConstants.FS_S3A_COMMITTER_STAGING_THREADS, 1);
    conf.set(PathOutputCommitterFactory.OUTPUTCOMMITTER_FACTORY_CLASS,
        DIRECTORY_COMMITTER_FACTORY);
    // disable unique filenames so that the protocol tests of FileOutputFormat
    // and this test generate consistent names.
    //conf.setBoolean(COMMITTER_UNIQUE_FILENAMES, true);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();

    // identify working dir for staging and delete
    Configuration conf = getConfiguration();
    String uuid = StagingS3GuardCommitter.getUploadUUID(conf,
        taskAttempt0.getJobID());
    Path tempDir = Paths.getLocalTaskAttemptTempDir(conf, uuid, taskAttempt0);
    rmdir(tempDir, conf);
  }

  @Override
  protected AbstractS3GuardCommitter createCommitter(
      TaskAttemptContext context) throws IOException {
    return new StagingS3GuardCommitter(getOutDir(), context);
  }

  @Override
  public AbstractS3GuardCommitter createCommitter(JobContext context)
      throws IOException {
    return new StagingS3GuardCommitter(getOutDir(), context);
  }

  public AbstractS3GuardCommitter createFailingCommitter(
      TaskAttemptContext tContext) throws IOException {
    return new CommitterWithFailedThenSucceed(getOutDir(), tContext);
  }

  @Override
  protected boolean shouldExpectSuccessMarker() {
    return false;
  }

  @Override
  protected IOException expectJobCommitToFail(JobContext jContext,
      AbstractS3GuardCommitter committer) throws Exception {
    return expectJobCommitFailure(jContext, committer,
        IOException.class);
  }

  /**
   * The class provides a overridden implementation of commitJobInternal which
   * causes the commit failed for the first time then succeed.
   */
  private static final class CommitterWithFailedThenSucceed extends
      StagingS3GuardCommitter implements FaultInjection {

    private final FaultInjectionImpl injection = new FaultInjectionImpl(true);

    CommitterWithFailedThenSucceed(Path outputPath,
        JobContext context) throws IOException {
      super(outputPath, context);
    }

    @Override
    public void setupJob(JobContext context) throws IOException {
      injection.setupJob(context);
      super.setupJob(context);
    }

    @Override
    public void abortJob(JobContext context, JobStatus.State state)
        throws IOException {
      injection.abortJob(context, state);
      super.abortJob(context, state);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void cleanupJob(JobContext context) throws IOException {
      injection.cleanupJob(context);
      super.cleanupJob(context);
    }

    @Override
    public void setupTask(TaskAttemptContext context) throws IOException {
      injection.setupTask(context);
      super.setupTask(context);
    }

    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {
      injection.commitTask(context);
      super.commitTask(context);
    }

    @Override
    public void abortTask(TaskAttemptContext context) throws IOException {
      injection.abortTask(context);
      super.abortTask(context);
    }

    @Override
    public void commitJob(JobContext context) throws IOException {
      injection.commitJob(context);
      super.commitJob(context);
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext context)
        throws IOException {
      injection.needsTaskCommit(context);
      return super.needsTaskCommit(context);
    }

    @Override
    public void setFaults(Faults... faults) {
      injection.setFaults(faults);
    }
  }
}
