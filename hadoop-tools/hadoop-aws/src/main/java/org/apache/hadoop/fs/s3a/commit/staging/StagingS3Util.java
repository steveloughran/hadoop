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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AUtils;
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

  private static final Logger LOG = LoggerFactory.getLogger(StagingS3Util.class);

  /**
   * Revert a pending commit by deleting the destination.
   * @param actions commit actions to use
   * @param commit pending
   * @throws IOException failure
   */
  public static void revertCommit(FileCommitActions actions,
      SinglePendingCommit commit) throws IOException {
    LOG.debug("Revert {}", commit);
    try {
      actions.getS3Client().deleteObject(commit.newDeleteRequest());
    } catch (AmazonClientException e) {
      throw S3AUtils.translateException("revert commit", commit.destinationKey, e);
    }
  }

  /**
   * Finish the pending commit.
   * @param actions commit actions to use
   * @param commit pending
   * @throws IOException failure
   */
  public static void finishCommit(FileCommitActions actions,
      SinglePendingCommit commit) throws IOException {
    LOG.debug("Finish {}", commit);
    try {
      actions.getS3Client().completeMultipartUpload(commit.newCompleteRequest());
    } catch (AmazonClientException e) {
      throw S3AUtils.translateException("complete commit",
          commit.destinationKey, e);
    }
  }

  /**
   * Abort a pending commit.
   * @param actions commit actions to use
   * @param pending pending commit to abort
   * @throws IOException failure
   */
  public static void abortCommit(FileCommitActions actions,
      SinglePendingCommit pending) throws IOException {
    LOG.debug("Abort {}", pending);
    abort(actions.getS3Client(), pending.destinationKey, pending.newAbortRequest());
  }

  /**
   * Abort an MPU; translate the failure into an IOE.
   * @param client S3 client
   * @param key dest key
   * @param request MPU request
   * @throws IOException failure
   */
  protected static void abort(AmazonS3 client,
      String key,
      AbortMultipartUploadRequest request) throws IOException {
    try {
      client.abortMultipartUpload(request);
    } catch (AmazonClientException e) {
      throw S3AUtils.translateException("abort commit", key, e);
    }
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

    LOG.debug("Initiating multipart upload from {} to s3a://{}/{}" +
            " partition={} partSize={}",
        localFile, bucket, key, partition, uploadPartSize);
    if (!localFile.exists()) {
      throw new FileNotFoundException(localFile.toString());
    }
    if (!localFile.isFile()) {
      throw new IOException("Not a file: " + localFile);
    }
    AmazonS3 client = actions.getS3Client();
    String uploadId = null;

    boolean threw = true;
    try {
      InitiateMultipartUploadResult initiate = client.initiateMultipartUpload(
          new InitiateMultipartUploadRequest(bucket, key));
      uploadId = initiate.getUploadId();
      long length = localFile.length();

      SinglePendingCommit commitData = new SinglePendingCommit();
      commitData.destinationKey = key;
      commitData.bucket = bucket;
      commitData.touch(System.currentTimeMillis());
      commitData.uploadId = uploadId;
      commitData.uri = destURI;
      commitData.text = partition != null ? "partition: " + partition :  "";
      commitData.size = length;

      Map<Integer, String> etags = Maps.newLinkedHashMap();

      long offset = 0;
      long numParts = (length / uploadPartSize +
          ((length % uploadPartSize) > 0 ? 1 : 0));

      List<PartETag> parts = new ArrayList<>((int)numParts);

      LOG.debug("File size is {}, number of parts to upload = {}",
          length, numParts);
/*
      Preconditions.checkArgument(numParts > 0,
          "Cannot upload 0 byte file: " + localFile);
*/

      for (int partNumber = 1; partNumber <= numParts; partNumber += 1) {
        long size = Math.min(length - offset, uploadPartSize);
        UploadPartRequest part = new UploadPartRequest()
            .withBucketName(bucket)
            .withKey(key)
            .withPartNumber(partNumber)
            .withUploadId(uploadId)
            .withFile(localFile)
            .withFileOffset(offset)
            .withPartSize(size)
            .withLastPart(partNumber == numParts);

        UploadPartResult partResult = client.uploadPart(part);
        PartETag etag = partResult.getPartETag();
        etags.put(etag.getPartNumber(), etag.getETag());
        offset += uploadPartSize;
        parts.add(etag);
      }

      commitData.bindCommitData(parts);

      threw = false;

      return commitData;
    } catch (AmazonClientException e) {
      throw S3AUtils.translateException("multipart upload", key, e);

    } finally {
      if (threw && uploadId != null) {
        try {
          abort(client, key,
              new AbortMultipartUploadRequest(bucket, key, uploadId));
        } catch (IOException e) {
          LOG.error("Failed to abort upload {} to {}", uploadId, key, e);
        }
      }
    }
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
    LOG.debug("Reading pending commits in file {}", pendingCommitsFile);
    return MultiplePendingCommits.getSerializer().load(fs, pendingCommitsFile);
  }

}
