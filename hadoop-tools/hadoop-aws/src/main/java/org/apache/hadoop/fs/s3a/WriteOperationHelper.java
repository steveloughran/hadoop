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

package org.apache.hadoop.fs.s3a;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.S3AUtils.translateException;


/**
 * Helper for low-level operations against an S3 Bucket for writing data
 * and creating and committing pending writes.
 * <p>
 * It hides direct access to the S3 API
 * and is a location where the object upload process can be evolved/enhanced.
 * <p>
 * Features
 * <ul>
 *   <li>Methods to create and submit requests to S3, so avoiding
 *   all direct interaction with the AWS APIs.</li>
 *   <li>Some extra preflight checks of arguments, so failing fast on
 *   errors.</li>
 *   <li>Callbacks to let the FS know of events in the output stream
 *   upload process.</li>
 *   <li>Failure handling, including converting exceptions to IOEs</li>
 *   <li>Integration with instrumentation and S3Guard<./li>
 * </ul>
 *
 * This API is for internal use only.
 */
@InterfaceAudience.Private
public class WriteOperationHelper {
  private static final Logger LOG =
      LoggerFactory.getLogger(WriteOperationHelper.class);
  private final S3AFileSystem owner;
  private final String key;
  private final AwsCall calls = new AwsCall();

  protected WriteOperationHelper(S3AFileSystem owner, String key) {
    checkArgument(key != null, "No key");
    this.owner = owner;
    this.key = key;
  }

  /**
   * Create a {@link PutObjectRequest} request.
   * If {@code length} is set, the metadata is configured with the size of
   * the upload.
   * @param inputStream source data.
   * @param length size, if known. Use -1 for not known
   * @return the request
   */
  public PutObjectRequest newPutRequest(InputStream inputStream,
      long length) {
    return createPutObjectRequest(key, inputStream, length);
  }

  /**
   * Create a {@link PutObjectRequest} request against the specific key.
   * @param destKey destination key
   * @param inputStream source data.
   * @param length size, if known. Use -1 for not known
   * @return the request
   */
  public PutObjectRequest createPutObjectRequest(String destKey,
      InputStream inputStream, long length) {
    return owner.newPutObjectRequest(destKey,
        newObjectMetadata(length),
        inputStream);
  }

  /**
   * Create a {@link PutObjectRequest} request to upload a file.
   * @param sourceFile source file
   * @return the request
   */
  public PutObjectRequest newPutRequest(File sourceFile) {
    return createPutObjectRequest(key, sourceFile);
  }

  /**
   * Create a {@link PutObjectRequest} request to upload a file.
   * @param dest key to PUT to.
   * @param sourceFile source file
   * @return the request
   */
  public PutObjectRequest createPutObjectRequest(String dest,
      File sourceFile) {
    int length = (int) sourceFile.length();
    PutObjectRequest request = owner.newPutObjectRequest(dest,
        newObjectMetadata(length), sourceFile);
    return request;
  }

  /**
   * Callback on a successful write.
   * @param length length of the write
   */
  public void writeSuccessful(long length) {
    LOG.debug("Successful write to {}, len {}", key, length);
  }

  /**
   * Callback on a write failure.
   * @param e Any exception raised which triggered the failure.
   */
  public void writeFailed(Exception e) {
    LOG.debug("Write to {} failed", this, e);
  }

  /**
   * Create a new object metadata instance.
   * Any standard metadata headers are added here, for example:
   * encryption.
   * @param length size, if known. Use -1 for not known
   * @return a new metadata instance
   */
  public ObjectMetadata newObjectMetadata(long length) {
    return owner.newObjectMetadata(length);
  }

  /**
   * Start the multipart upload process.
   * @return the upload result containing the ID
   * @throws IOException IO problem
   */
  public String initiateMultiPartUpload() throws IOException {
    LOG.debug("Initiating Multipart upload to {}", key);
    final InitiateMultipartUploadRequest initiateMPURequest =
        new InitiateMultipartUploadRequest(owner.getBucket(),
            key,
            newObjectMetadata(-1));
    initiateMPURequest.setCannedACL(owner.getCannedACL());
    owner.setOptionalMultipartUploadRequestParameters(initiateMPURequest);

    return calls.execute("initiate MultiPartUpload", key,
        () ->
            owner.getAmazonS3Client()
                .initiateMultipartUpload(initiateMPURequest).getUploadId());
  }

  /**
   * Abort a multipart upload operation.
   * @param dest destination key of upload
   * @param uploadId multipart operation Id
   * @throws IOException on problems.
   */
  public void abortMultipartCommit(String dest, String uploadId)
      throws IOException {
    calls.execute("aborting multipart commit", dest,
        () -> abortMultipartUpload(dest, uploadId));
  }

  /**
   * Abort a multipart upload operation.
   * @param upload multipart upload
   * @throws IOException on problems.
   */
  public void abortMultipartCommit(MultipartUpload upload)
      throws IOException {
    abortMultipartCommit(upload.getKey(), upload.getUploadId());
  }

  /**
   * Complete the multipart commit operation.
   * @param destination destination of the commit
   * @param uploadId multipart operation Id
   * @param partETags list of partial uploads
   * @return the result of the operation.
   * @throws IOException on problems.
   */
  public CompleteMultipartUploadResult completeMultipartCommit(
      String destination,
      String uploadId,
      List<PartETag> partETags,
      long length) throws IOException {
    return finalizeMultipartUpload(destination, uploadId, partETags, length);
  }

  /**
   * Finalize a multipart PUT operation.
   * This completes the upload, and, if that works, calls
   * {@link S3AFileSystem#finishedWrite(String, long)} to update the filesystem.
   * @param uploadId multipart operation Id
   * @param partETags list of partial uploads
   * @return the result of the operation.
   * @throws IOException on problems.
   */
  public CompleteMultipartUploadResult finalizeMultipartUpload(
      String destination,
      String uploadId,
      List<PartETag> partETags,
      long length) throws IOException {
    return calls.execute("Completing multipart commit", destination,
        () -> {
          CompleteMultipartUploadResult result
              = completeMultipartUpload(uploadId, partETags);
          finishedWrite(destination, length);
          return result;
        });
  }

  /**
   * Report to the owner that the write has finished; this may update
   * any metastore.
   * @param destination destination path
   * @param size size of committed write.
   */
  private void finishedWrite(String destination, long size) {
    owner.finishedWrite(destination, size);
  }

  /**
   * Complete a multipart upload operation.
   * This does not finalize the write.
   * @param uploadId multipart operation Id
   * @param partETags list of partial uploads
   * @return the result of the operation.
   * @throws AmazonClientException on problems.
   */
  private CompleteMultipartUploadResult completeMultipartUpload(
      String uploadId,
      List<PartETag> partETags) throws AmazonClientException {
    checkNotNull(uploadId);
    checkNotNull(partETags);
    LOG.debug("Completing multipart upload {} with {} parts",
        uploadId, partETags.size());
    // a copy of the list is required, so that the AWS SDK doesn't
    // attempt to sort an unmodifiable list.
    return owner.getAmazonS3Client().completeMultipartUpload(
        new CompleteMultipartUploadRequest(owner.getBucket(),
            key,
            uploadId,
            new ArrayList<>(partETags)));
  }

  /**
   * Abort a multipart upload operation.
   * @param uploadId multipart operation Id
   * @throws AmazonClientException on problems.
   */
  public void abortMultipartUpload(String uploadKey, String uploadId)
      throws AmazonClientException {
    LOG.debug("Aborting multipart upload {} to {}", uploadId, uploadKey);
    owner.getAmazonS3Client().abortMultipartUpload(
        new AbortMultipartUploadRequest(owner.getBucket(), uploadKey,
            uploadId));
  }

  /**
   * Abort all multipart uploads under a path.
   * @param prefix prefix for uploads to abort
   * @return a count of aborts
   * @throws IOException trouble. FileNotFoundExceptions are swallowed.
   */
  public int abortMultipartUploadsUnderPath(String prefix)
      throws IOException {
    int count = 0;
    for (MultipartUpload upload : owner.listMultipartUploads(prefix)) {
      try {
        abortMultipartCommit(upload);
        count++;
      } catch (FileNotFoundException e) {
        LOG.debug("Already aborted: {}", upload.getKey(), e);
      }
    }
    return count;
  }

  /**
   * Create and initialize a part request of a multipart upload.
   * Exactly one of: {@code uploadStream} or {@code sourceFile}
   * must be specified.
   * A subset of the file may be posted, by providing the starting point
   * in {@code offset} and a length of block in {@code size} equal to
   * or less than the remaining bytes.
   * @param uploadId ID of ongoing upload
   * @param partNumber current part number of the upload
   * @param size amount of data
   * @param uploadStream source of data to upload
   * @param sourceFile optional source file.
   * @param offset offset in file to start reading.
   * @return the request.
   */
  public UploadPartRequest newUploadPartRequest(String uploadId,
      int partNumber,
      int size,
      InputStream uploadStream,
      File sourceFile,
      Long offset) {
    checkNotNull(uploadId);
    // exactly one source must be set; xor verifies this
    checkArgument((uploadStream != null) ^ (sourceFile != null),
        "Data source");
    checkArgument(size >= 0, "Invalid partition size %s", size);
    checkArgument(partNumber > 0 && partNumber <= 10000,
        "partNumber must be between 1 and 10000 inclusive, but is %s",
        partNumber);

    LOG.debug("Creating part upload request for {} #{} size {}",
        uploadId, partNumber, size);
    UploadPartRequest request = new UploadPartRequest()
        .withBucketName(owner.getBucket())
        .withKey(key)
        .withUploadId(uploadId)
        .withPartNumber(partNumber)
        .withPartSize(size);
    if (uploadStream != null) {
      // there's an upload stream. Bind to it.
      request.setInputStream(uploadStream);
    } else {
      checkArgument(sourceFile.exists(),
          "Source file does not exist: %s", sourceFile);
      checkArgument(offset >= 0, "Invalid offset %s", offset);
      long length = sourceFile.length();
      checkArgument(offset == 0 || offset < length,
          "Offset %s beyond length of file %s", offset, length);
      request.setFile(sourceFile);
      request.setFileOffset(offset);
    }
    return request;
  }

  /**
   * The toString method is intended to be used in logging/toString calls.
   * @return a string description.
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "WriteOperationHelper {bucket=").append(owner.getBucket());
    sb.append(", key='").append(key).append('\'');
    sb.append('}');
    return sb.toString();
  }

  /**
   * PUT an object directly (i.e. not via the transfer manager).
   * Byte length is calculated from the file length, or, if there is no
   * file, from the content length of the header.
   * @param putObjectRequest the request
   * @return the upload initiated
   * @throws IOException on problems
   */
  public PutObjectResult putObject(PutObjectRequest putObjectRequest)
      throws IOException {
    return calls.execute("put", putObjectRequest.getKey(),
        () -> owner.putObjectDirect(putObjectRequest));
  }


  /**
   * Revert a commit by deleting the file.
   * TODO: Policy regarding creating a mock empty parent directory.
   * @param destKey destination key
   * @throws IOException due to inability to delete a directory or file.
   */
  public boolean revertCommit(String destKey) throws IOException {
    try {
      calls.execute("revert commit", destKey,
          () ->
              owner.deleteObjectAtPath(owner.keyToPath(destKey),
                  destKey, true));
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Upload part of a multi-partition file.
   * @param request request
   * @return the result of the operation.
   * @throws IOException on problems
   */
  public UploadPartResult uploadPart(UploadPartRequest request)
      throws IOException {
    return calls.execute("upload part",
        request.getKey(),
        () -> owner.uploadPart(request));
  }
}
