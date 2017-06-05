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

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.CommitOperations;

/**
 * Extension of {@link CommitOperations} for mocking S3A in tests.
 */
public class MockFileCommitOperations extends CommitOperations {

  /**
   * Creator.
   * @param fs filesystem to work with.
   */
  public MockFileCommitOperations(S3AFileSystem fs) {
    super(fs);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "MockFileCommitOperations{");
    sb.append(super.toString());
    sb.append('}');
    return sb.toString();
  }
}
