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
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.AbstractS3GuardCommitter;
import org.apache.hadoop.fs.s3a.commit.FileCommitActions;

import java.io.IOException;

/**
 * Extension of {@link FileCommitActions} for mocking S3A in tests.
 */
public class MockFileCommitActions extends FileCommitActions {

  private final StagingTestBase.ClientResults
      results = new StagingTestBase.ClientResults();
  private final StagingTestBase.ClientErrors
      errors = new StagingTestBase.ClientErrors();
  private final AmazonS3 mockClient;

  public MockFileCommitActions(S3AFileSystem fs, AmazonS3 mockClient) {
    super(fs);
    if (mockClient != null) {
      this.mockClient = mockClient;
    } else {
      this.mockClient = StagingTestBase.newMockClient(
          getResults(), getErrors());
    }
  }

  public StagingTestBase.ClientResults getResults() {
    return results;
  }

  public StagingTestBase.ClientErrors getErrors() {
    return errors;
  }

  @Override
  public AmazonS3 getS3Client() {
    return mockClient;
  }
}
