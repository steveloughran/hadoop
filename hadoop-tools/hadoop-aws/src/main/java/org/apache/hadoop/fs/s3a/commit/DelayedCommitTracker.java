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

package org.apache.hadoop.fs.s3a.commit;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;

/**
 * Put tracker for delayed commits.
 */
@InterfaceAudience.Private
public class DelayedCommitTracker extends DefaultPutTracker {
  public static final Logger LOG = LoggerFactory.getLogger(
      DelayedCommitTracker.class);

  private final String pendingPartKey;
  private final Path path;
  private final WriteOperationHelper writer;
  private final String bucket;

  /**
   * Delayed commit tracker.
   * @param path path nominally being written to
   * @param bucket dest bucket
   * @param destKey key for the destination
   * @param pendingPartKey key of the pending part
   * @param writer writer instance to use for operations
   */
  public DelayedCommitTracker(Path path,
      String bucket,
      String destKey,
      String pendingPartKey,
      WriteOperationHelper writer) {
    super(destKey);
    this.bucket = bucket;
    this.path = path;
    this.pendingPartKey = pendingPartKey;
    this.writer = writer;
  }

  /**
   * Initialize the tracker.
   * @return true, indicating that the multipart commit must start.
   * @throws IOException any IO problem.
   */
  @Override
  public boolean inited() throws IOException {
    return true;
  }

  /**
   * Complete operation: generate the final commit data, put it.
   * @param uploadId Upload ID
   * @param parts list of parts
   * @param bytesWritten bytes written
   * @return false, indicating that the commit must fail.
   * @throws IOException any IO problem.
   * @throws IllegalArgumentException bad argument
   */
  @Override
  public boolean aboutToComplete(String uploadId,
      List<PartETag> parts,
      long bytesWritten)
      throws IOException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(uploadId),
        "empty/null upload ID: "+ uploadId);
    Preconditions.checkArgument(parts != null,
        "No uploaded parts list");
    Preconditions.checkArgument(!parts.isEmpty(),
        "No uploaded parts to save");
    SinglePendingCommit commitData = new SinglePendingCommit();
    commitData.touch(System.currentTimeMillis());
    commitData.setDestinationKey(getDestKey());
    commitData.setBucket(bucket);
    commitData.setUri(path.toUri().toString());
    commitData.setUploadId(uploadId);
    commitData.setText("");
    commitData.setSize(bytesWritten);
    commitData.bindCommitData(parts);
    byte[] bytes = commitData.toBytes();
    PutObjectRequest put = writer.newPutRequest(
        new ByteArrayInputStream(bytes), bytes.length);
    writer.uploadObject(put);
    LOG.debug("{} — put delayed commit information to {}:\n{}",
        this, path, commitData);
    return false;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "DelayedCompleteTracker{");
    sb.append(", destKey=").append(getDestKey());
    sb.append(", pendingPartKey='").append(pendingPartKey).append('\'');
    sb.append(", path=").append(path);
    sb.append(", writer=").append(writer);
    sb.append('}');
    return sb.toString();
  }
}
