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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.fs.s3a.commit.staging;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.FileCommitActions;
import org.apache.hadoop.fs.s3a.commit.MultiplePendingCommits;
import org.apache.hadoop.fs.s3a.commit.SinglePendingCommit;

/**
 * Low level S3 integration.
 * This is slowly moving towards delegating work to the S3A FS by way of
 * {@link FileCommitActions}.
 */
public final class StagingS3Util {

  private StagingS3Util() {
  }

  /**
   * Revert a pending commit by deleting the destination.
   * @param actions commit actions to use
   * @param commit pending
   * @throws IOException failure
   */
  public static void revertCommit(FileCommitActions actions,
      SinglePendingCommit commit) throws IOException {
    actions.revertCommit(commit);
  }

  /**
   * Finish the pending commit.
   * @param actions commit actions to use
   * @param commit pending
   * @throws IOException failure
   */
  public static void finishCommit(FileCommitActions actions,
      SinglePendingCommit commit) throws IOException {
    actions.commitOrFail(commit);
  }

  /**
   * Abort a pending commit.
   * @param actions commit actions to use
   * @param pending pending commit to abort
   * @throws IOException failure
   */
  public static void abortCommit(FileCommitActions actions,
      SinglePendingCommit pending) throws IOException {
    actions.abortMultipartCommit(pending);
  }

  /**
   * Upload all the data in the local file, returning the information
   * needed to commit the work.
   * @param actions commit actions to use
   * @param localFile local file (be  a file)
   * @param partition partition/subdir. Not used
   * @param bucket dest bucket
   * @param key dest key
   * @param destURI destination
   * @param uploadPartSize size of upload  @return a pending upload entry
   * @return the commit data
   * @throws IOException failure
   */
  public static SinglePendingCommit multipartUpload(
      FileCommitActions actions, File localFile, String partition,
      String bucket, String key, String destURI, long uploadPartSize)
      throws IOException {
    return actions.uploadFileToPendingCommit(localFile, partition, bucket,
        key, destURI,
        uploadPartSize);
  }

  /**
   * deserialize a file of pending commits.
   * @param fs filesystem
   * @param pendingCommitsFile filename
   * @return the list of uploads
   * @throws IOException IO Failure
   */
  static MultiplePendingCommits readPendingCommits(FileSystem fs,
      Path pendingCommitsFile) throws IOException {
    return MultiplePendingCommits.load(fs, pendingCommitsFile);
  }

}
