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
import org.apache.hadoop.fs.s3a.commit.FileCommitActions;

/**
 * Extension of {@link FileCommitActions} for mocking S3A in tests.
 */
public class MockFileCommitActions extends FileCommitActions {

  private final StagingTestBase.ClientResults
      results = new StagingTestBase.ClientResults();
  private final StagingTestBase.ClientErrors
      errors = new StagingTestBase.ClientErrors();
  private final AmazonS3 mockClient;

  /**
   * Creator. Will create a mock S3 client if none is provided, using the
   * results and errors in this instance. Can optionally patch the FS
   * with the mock client.
   * @param fs filesystem to work with.
   * @param mockClient optional mock S3A FS: if unset, one is created.
   * @param patchFSwithMockS3Client set flag to patch the FS if the client was
   * created on demand' ignored if a client is passed in (caller is expected
   * to have set this up)
   */
  public MockFileCommitActions(S3AFileSystem fs,
      AmazonS3 mockClient,
      boolean patchFSwithMockS3Client) {
    super(fs);
    if (mockClient != null) {
      this.mockClient = mockClient;
    } else {
      this.mockClient = StagingTestBase.newMockS3Client(
          getResults(), getErrors());
      // conditionally patch any mock FS with the created client
      if (patchFSwithMockS3Client) {
        ((MockS3AFileSystem) fs).setAmazonS3Client(this.mockClient);
      }
    }
  }

  public StagingTestBase.ClientResults getResults() {
    return results;
  }

  public StagingTestBase.ClientErrors getErrors() {
    return errors;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "MockFileCommitActions{");
    sb.append("results=").append(results);
    sb.append(", errors=").append(errors);
    sb.append('}');
    return sb.toString();
  }
}
