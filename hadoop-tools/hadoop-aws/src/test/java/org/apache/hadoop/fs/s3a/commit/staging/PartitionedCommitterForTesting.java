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

import com.amazonaws.services.s3.AmazonS3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

class PartitionedCommitterForTesting extends
    PartitionedStagingCommitter {
  private final MockFileCommitActions mockCommitActions;
  private final AmazonS3 mockClient;
  private final S3AFileSystem mockFS;

  PartitionedCommitterForTesting(Path outputPath,
      TaskAttemptContext context,
      AmazonS3 mockClient,
      S3AFileSystem mockFS) throws IOException {
    super(outputPath, context);
    this.mockFS = mockFS;
    this.mockClient = mockClient;
    mockCommitActions = new MockFileCommitActions(getDestS3AFS(), mockClient,
        true);
    setCommitActions(mockCommitActions);
  }

  PartitionedCommitterForTesting(Path outputPath,
      JobContext context,
      AmazonS3 mockClient,
      S3AFileSystem mockFS) throws IOException {
    super(outputPath, context);
    this.mockFS = mockFS;
    this.mockClient = mockClient;
    mockCommitActions = new MockFileCommitActions(getDestS3AFS(), mockClient,
        true);
    setCommitActions(mockCommitActions);
  }

  @Override
  protected void initOutput(Path out) throws IOException {
    super.initOutput(out);
    setOutputPath(out);
  }

  @Override
  protected AmazonS3 getClient(Path path, Configuration conf) {
    return mockClient;
  }

  /**
   * Returns the mock FS without checking FS type.
   * @param out output path
   * @param config job/task config
   * @return a filesystem.
   * @throws IOException
   */
  @Override
  protected FileSystem getDestination(Path out, Configuration config)
      throws IOException {
    return out.getFileSystem(config);
  }

}
