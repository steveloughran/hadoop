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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.commit.SinglePendingCommit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * TODO: All this needs to be replaced by delegating to S3AFS.
 */
public final class S3Util {

  private S3Util() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(S3Util.class);

  /**
   * Revert a pending commit by deleting the destination.
   * @param client client
   * @param commit pending
   * @throws IOException failure
   */
  public static void revertCommit(AmazonS3 client,
      PendingUpload commit) throws IOException {
    LOG.debug("Revert {}", commit);
    try {
      client.deleteObject(commit.newDeleteRequest());
    } catch (AmazonClientException e) {
      throw S3AUtils.translateException("revert commit", commit.getKey(), e);
    }
  }

  /**
   * Finish the pending commit.
   * @param client client
   * @param commit pending
   * @throws IOException failure
   */
  public static void finishCommit(AmazonS3 client,
      PendingUpload commit) throws IOException {
    LOG.debug("Finish {}", commit);
    try {
      client.completeMultipartUpload(commit.newCompleteRequest());
    } catch (AmazonClientException e) {
      throw S3AUtils.translateException("complete commit", commit.getKey(), e);
    }
  }

  private static CompleteMultipartUploadRequest newCompleteRequest(
      PendingUpload commit) {
    List<PartETag> etags = Lists.newArrayList();
    for (Map.Entry<Integer, String> entry : commit.parts.entrySet()) {
      etags.add(new PartETag(entry.getKey(), entry.getValue()));
    }

    return new CompleteMultipartUploadRequest(
        commit.bucket, commit.key, commit.uploadId, etags);
  }

  /**
   * Abort a pending commit.
   * @param client client
   * @param pending pending commit to abort
   * @throws IOException failure
   */
  public static void abortCommit(AmazonS3 client,
      PendingUpload pending) throws IOException {
    LOG.debug("Abort {}", pending);
    abort(client, pending.getKey(), pending.newAbortRequest());
  }

  /**
   * Abort an MPU.
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
   * @param client S3 client
   * @param localFile local file (be  a file)
   * @param partition partition/subdir. Not used
   * @param bucket dest bucket
   * @param key dest key
   * @param destURI
   *@param uploadPartSize size of upload  @return a pending upload entry
   * @throws IOException failure
   */
  public static PendingUpload multipartUpload(
      AmazonS3 client, File localFile, String partition,
      String bucket, String key, String destURI, long uploadPartSize)
      throws IOException {

    LOG.debug("Initiating multipart upload from {} to s3a://{}/{} partition={}",
        localFile, bucket, key, partition);
    if (!localFile.exists()) {
      throw new FileNotFoundException(localFile.toString());
    }
    if (!localFile.isFile()) {
      throw new IOException("Not a file: " + localFile);
    }

    String uploadId = null;

    boolean threw = true;
    try {
      InitiateMultipartUploadResult initiate = client.initiateMultipartUpload(
          new InitiateMultipartUploadRequest(bucket, key));
      uploadId = initiate.getUploadId();
      long length = localFile.length();

      SinglePendingCommit commitData = new SinglePendingCommit();
      commitData.destinationKey = key;
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

      PendingUpload pending = new PendingUpload(
          partition, bucket, key, uploadId, etags);

      threw = false;

      return pending;
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
  static List<PendingUpload> readPendingCommits(FileSystem fs,
      Path pendingCommitsFile) throws IOException {
    LOG.debug("Reading pending commits in file {}", pendingCommitsFile);
    List<PendingUpload> commits = Lists.newArrayList();

    ObjectInputStream in = new ObjectInputStream(fs.open(pendingCommitsFile));
    boolean threw = true;
    try {
      for (PendingUpload commit : new ObjectIterator<PendingUpload>(in)) {
        commits.add(commit);
      }
      threw = false;
    } finally {
      Closeables.close(in, threw);
    }

    return commits;
  }

  /**
   * Iterator over objects in an object stream.
   * @param <T> type of object.
   */
  private static final class ObjectIterator<T> implements Iterator<T>, Iterable<T> {
    private final ObjectInputStream stream;
    private boolean hasNext;
    private T next;

    public ObjectIterator(ObjectInputStream stream) {
      this.stream = stream;
      readNext();
    }

    @Override
    public Iterator<T> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return hasNext;
    }

    @Override
    public T next() {
      T toReturn = next;
      this.readNext();
      return toReturn;
    }

    @SuppressWarnings("unchecked")
    public void readNext() {
      try {
        this.next = (T) stream.readObject();
        this.hasNext = (next != null);
      } catch (EOFException e) {
        this.hasNext = false;
      } catch (ClassNotFoundException | IOException e) {
        this.hasNext = false;
        Throwables.propagate(e);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Not implemented");
    }
  }

  /**
   * This class is used to pass information about pending uploads from tasks to
   * the job committer. It is serializable and will instantiate S3 requests.
   */
  public static final class PendingUpload implements Serializable {
    private final String partition;
    private final String bucket;
    private final String key;
    private final String uploadId;
    private final SortedMap<Integer, String> parts;

    public PendingUpload(String partition,
        String bucket,
        String key,
        String uploadId,
        Map<Integer, String> etags) {
      this.partition = partition;
      this.bucket = bucket;
      this.key = key;
      this.uploadId = uploadId;
      this.parts = ImmutableSortedMap.copyOf(etags);
    }

    public PendingUpload(String bucket,
        String key,
        String uploadId,
        List<PartETag> etags) {
      this.partition = null;
      this.bucket = bucket;
      this.key = key;
      this.uploadId = uploadId;

      ImmutableSortedMap.Builder<Integer, String> builder =
          ImmutableSortedMap.builder();
      for (PartETag etag : etags) {
        builder.put(etag.getPartNumber(), etag.getETag());
      }

      this.parts = builder.build();
    }

    public CompleteMultipartUploadRequest newCompleteRequest() {
      List<PartETag> etags = Lists.newArrayList();
      for (Map.Entry<Integer, String> entry : parts.entrySet()) {
        etags.add(new PartETag(entry.getKey(), entry.getValue()));
      }

      return new CompleteMultipartUploadRequest(
          bucket, key, uploadId, etags);
    }

    public DeleteObjectRequest newDeleteRequest() {
      return new DeleteObjectRequest(bucket, key);
    }

    public AbortMultipartUploadRequest newAbortRequest() {
      return new AbortMultipartUploadRequest(bucket, key, uploadId);
    }

    public String getBucketName() {
      return bucket;
    }

    public String getKey() {
      return key;
    }

    public String getUploadId() {
      return uploadId;
    }

    public SortedMap<Integer, String> getParts() {
      return parts;
    }

    public String getPartition() {
      return partition;
    }

    public String getLocation() {
      return Paths.getParent(key);
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "PendingUpload{");
      sb.append(" to \'s3a://").append(bucket).append('/');
      sb.append(key).append('\'');
      sb.append(" , uploadId='").append(uploadId).append('\'');
      sb.append('}');
      return sb.toString();
    }
  }
}
