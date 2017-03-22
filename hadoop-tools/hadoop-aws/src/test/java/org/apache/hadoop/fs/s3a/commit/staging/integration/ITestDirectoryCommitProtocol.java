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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.AbstractS3GuardCommitter;
import org.apache.hadoop.fs.s3a.commit.staging.DirectoryStagingCommitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/** ITest of the low level protocol methods. */
public class ITestDirectoryCommitProtocol extends ITestStagingCommitProtocol {

  @Override
  protected String suitename() {
    return "ITestDirectoryCommitProtocol";
  }

  @Override
  protected AbstractS3GuardCommitter createCommitter(TaskAttemptContext context)
      throws IOException {
    return new DirectoryStagingCommitter(outDir, context);
  }

  @Override
  public AbstractS3GuardCommitter createCommitter(JobContext context)
      throws IOException {
    return new DirectoryStagingCommitter(outDir, context);
  }


  @Override
  public AbstractS3GuardCommitter createFailingCommitter(
      TaskAttemptContext tContext) throws IOException {
    return new CommitterWithFailedThenSucceed(outDir, tContext);
  }

  /**
   * The class provides a overridden implementation of commitJobInternal which
   * causes the commit failed for the first time then succeed.
   */
  private static final class CommitterWithFailedThenSucceed extends
      DirectoryStagingCommitter {
    private final FaultInjection failure = new FaultInjection();

    CommitterWithFailedThenSucceed(Path outputPath,
        JobContext context) throws IOException {
      super(outputPath, context);
    }

    @Override
    public void commitJob(JobContext context) throws IOException {
      super.commitJob(context);
      failure.commitJob();
    }
  }
}
