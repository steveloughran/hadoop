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
import org.apache.hadoop.fs.s3a.commit.staging.StagingTestBase.ClientErrors;
import org.apache.hadoop.fs.s3a.commit.staging.StagingTestBase.ClientResults;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Committer subclass that uses a mocked AmazonS3Client for testing.
 */
class MockedStagingCommitter extends StagingS3GuardCommitter {

  public final ClientResults results = new ClientResults();
  public final ClientErrors errors = new ClientErrors();
  private final AmazonS3 mockClient = StagingTestBase.newMockClient(results, errors);

  public MockedStagingCommitter(Path outputPath, JobContext context)
      throws IOException {
    super(outputPath, context);
  }

  public MockedStagingCommitter(Path outputPath, TaskAttemptContext context)
      throws IOException {
    super(outputPath, context);
  }

  /**
   * Returns the mock FS without checking FS type.
   * @param out output path
   * @param conf job/task config
   * @return a filesystem.
   * @throws IOException
   */
  @Override
  protected FileSystem getDestination(Path out, Configuration conf)
      throws IOException {
    return out.getFileSystem(conf);
  }

  /*

  @Override
  protected void initOutput(Path outputPath) throws IOException {

    setOutputPath(outputPath);
  }
*/

  @Override
  protected AmazonS3 getClient(Path path, Configuration conf) {
    return mockClient;
  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    super.commitJob(context);
    Configuration conf = context.getConfiguration();
    try {
      String jobCommitterPath = conf.get("mock-results-file");
      if (jobCommitterPath != null) {
        try (ObjectOutputStream out = new ObjectOutputStream(
            FileSystem.getLocal(conf).create(new Path(jobCommitterPath), false))) {
          out.writeObject(results);
        }
      }
    } catch (Exception e) {
      // do nothing, the test will fail
    }
  }

}
