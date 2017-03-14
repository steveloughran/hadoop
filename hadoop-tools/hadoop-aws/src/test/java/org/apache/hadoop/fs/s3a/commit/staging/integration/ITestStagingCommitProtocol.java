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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.AbstractITCommitProtocol;
import org.apache.hadoop.fs.s3a.commit.AbstractS3GuardCommitter;
import org.apache.hadoop.fs.s3a.commit.staging.DirectoryStagingCommitterFactory;
import org.apache.hadoop.fs.s3a.commit.staging.Paths;
import org.apache.hadoop.fs.s3a.commit.staging.StagingS3GuardCommitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;

import java.io.IOException;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.COMMITTER_ENABLED;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingCommitterConstants.*;

public class ITestStagingCommitProtocol extends AbstractITCommitProtocol {

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setBoolean(COMMITTER_ENABLED, false);
    conf.setInt(COMMITTER_THREADS, 1);
    conf.set(PathOutputCommitterFactory.OUTPUTCOMMITTER_FACTORY_CLASS,
        DirectoryStagingCommitterFactory.NAME);
    // disable unique filenames so that the protocol tests of FileOutputFormat
    // and this test generate consistent names.
    conf.setBoolean(COMMITTER_UNIQUE_FILENAMES, false);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();

    // identify working dir for staging and delete
    Configuration conf = getConfiguration();
    String uuid = StagingS3GuardCommitter.getUploadUUID(conf,
        TASK_ATTEMPT_0.getJobID());
    Path tempDir = Paths.getLocalTaskAttemptTempDir(conf, uuid, TASK_ATTEMPT_0);
    rmdir(tempDir, conf);
  }

  @Override
  protected AbstractS3GuardCommitter createCommitter(TaskAttemptContext context)
      throws IOException {
    return new StagingS3GuardCommitter(outDir, context);
  }

  @Override
  public AbstractS3GuardCommitter createCommitter(JobContext context)
      throws IOException {
    return new StagingS3GuardCommitter(outDir, context);
  }


  public AbstractS3GuardCommitter createFailingCommitter(TaskAttemptContext tContext)
      throws IOException {
    return new CommitterWithFailedThenSucceed(outDir, tContext);
  }

  @Override
  protected boolean shouldExpectSuccessMarker() {
    return false;
  }

  @Override
  protected IOException expectSecondJobCommitToFail(JobContext jContext,
      AbstractS3GuardCommitter committer) throws Exception {
    return expectJobCommitFailure(jContext, committer,
        IOException.class);
  }

  /**
   * The class provides a overridden implementation of commitJobInternal which
   * causes the commit failed for the first time then succeed.
   */

  private static class CommitterWithFailedThenSucceed extends
      StagingS3GuardCommitter {
    private final FailThenSucceed failure = new FailThenSucceed();

    CommitterWithFailedThenSucceed(Path outputPath,
        JobContext context) throws IOException {
      super(outputPath, context);
    }

    @Override
    public void commitJob(JobContext context) throws IOException {
      super.commitJob(context);
      failure.exec();
    }
  }
}
