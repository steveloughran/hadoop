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

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.google.common.base.Preconditions;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.JsonSerDeser;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.util.StringUtils.join;

/**
 * This is the serialization format for the delayed commit operation.
 *
 * It's marked as {@link Serializable} so that it can be passed in RPC
 * calls; for this to work it relies on the fact that Java.io ArrayList
 * and LinkedList are serializable. If any other list type is used for etags,
 * it must also be serialized. Jackson expects lists, and it is used
 * to persist to disk.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SinglePendingCommit extends PersistentCommitData
    implements Iterable<String> {

  private static JsonSerDeser<SinglePendingCommit> serializer
      = new JsonSerDeser<>(SinglePendingCommit.class, false, true);

  // This type is serialized/deserilized by Jackson: make all the fields visible
  // to show what is going on.

  /**
   * Serialization ID: {@value}.
   */
  private static final long serialVersionUID = 0x10000 + VERSION;

  /** Version marker. */
  public int version = VERSION;

  /**
   * This is the filename of the pending file itself.
   * Used during processing; it's persistent value, if any, is ignored.
   */
  public String filename;

  /** Path URI of the destination. */
  public String uri = "";

  /** ID of the upload. */
  public String uploadId;

  /** Destination bucket. */
  public String bucket;

  /** Destination key in the bucket. */
  public String destinationKey;

  /** When was the upload created? */
  public long created;

  /** When was the upload saved? */
  public long saved;

  /** timestamp as date; no expectation of deserializability. */
  public String date;

  /** Job ID, if known. */
  public String jobId = "";

  /** Task ID, if known. */
  public String taskId = "";

  /** Arbitrary notes. */
  public String text = "";

  /** Ordered list of etags. */
  public List<String> etags;

  /**
   * Any custom extra data committer subclasses may choose to add.
   */
  public Map<String, String> extraData = new HashMap<>(0);

  /** Destination file size. */
  public long size;

  public SinglePendingCommit() {
  }

  /**
   * Deserialize via java Serialization API: deserialize the instance
   * and then call {@link #validate()} to verify that the deserialized
   * data is valid.
   * @param inStream input stream
   * @throws IOException IO problem
   * @throws ClassNotFoundException reflection problems
   * @throws IllegalStateException validation failure
   */
  private void readObject(ObjectInputStream inStream) throws IOException,
      ClassNotFoundException {
    inStream.defaultReadObject();
    validate();
  }

  /**
   * Set the various timestamp fields to the supplied value.
   * @param millis time in milliseconds
   */
  public void touch(long millis) {
    created = millis;
    saved = millis;
    date = new Date(millis).toString();
  }

  /**
   * Set the commit data.
   * @param parts ordered list of etags.
   */
  public void bindCommitData(List<PartETag> parts) {
    etags = new ArrayList<>(parts.size());
    int counter = 1;
    for (PartETag part : parts) {
      Preconditions.checkState(part.getPartNumber() == counter,
          "Expected part number %s but got %s", counter, part.getPartNumber());
      etags.add(part.getETag());
      counter++;
    }
  }

  @Override
  public void validate() {
    Preconditions.checkState(version == VERSION, "Wrong version: %s", version);
    Preconditions.checkState(StringUtils.isNotEmpty(bucket),
        "Empty bucket");
    Preconditions.checkState(StringUtils.isNotEmpty(destinationKey),
        "Empty destination");
    Preconditions.checkState(StringUtils.isNotEmpty(uploadId),
        "Empty uploadId");
    Preconditions.checkState(size >= 0, "Invalid size: " + size);
    destinationPath();
    Preconditions.checkState(etags != null, "No etag list");
//    Preconditions.checkState(!etags.isEmpty(), "Empty etag list");
    CommitUtils.validateCollectionClass(etags, String.class);
    for (String etag : etags) {
      Preconditions.checkState(StringUtils.isNotEmpty(etag), "Empty etag");
    }
    if (extraData != null) {
      CommitUtils.validateCollectionClass(extraData.keySet(), String.class);
      CommitUtils.validateCollectionClass(extraData.values(), String.class);
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "DelayedCompleteData{");
    sb.append("version=").append(version);
    sb.append(", uri='").append(uri).append('\'');
    sb.append(", destination='").append(destinationKey).append('\'');
    sb.append(", uploadId='").append(uploadId).append('\'');
    sb.append(", created=").append(created);
    sb.append(", saved=").append(saved);
    sb.append(", size=").append(size);
    sb.append(", date='").append(date).append('\'');
    sb.append(", jobId='").append(jobId).append('\'');
    sb.append(", taskId='").append(taskId).append('\'');
    sb.append(", notes='").append(text).append('\'');
    if (etags != null) {
      sb.append(", etags=[");
      sb.append(join(",", etags));
      sb.append(']');
    } else {
      sb.append(", etags=null");
    }
    sb.append('}');
    return sb.toString();
  }

  @Override
  public byte[] toBytes() throws IOException {
    validate();
    return getSerializer().toBytes(this);
  }

  @Override
  public void save(FileSystem fs, Path path, boolean overwrite)
      throws IOException {
    getSerializer().save(fs, path, this, overwrite);
  }

  /**
   * Get the singleton JSON serializer for this class.
   * @return the serializer.
   */
  public static JsonSerDeser<SinglePendingCommit> getSerializer() {
    return serializer;
  }

  /**
   * Create a completion request from the operation.
   * TODO: this is an intermediate operation
   * @return the rewuest
   */
  public CompleteMultipartUploadRequest newCompleteRequest() {
    validate();
    List<PartETag> parts = Lists.newArrayList();
    for (int i = 0; i < etags.size(); i++) {
      parts.add(new PartETag(i + 1, etags.get(i)));
    }
    return new CompleteMultipartUploadRequest(
        bucket, destinationKey, uploadId, parts);
  }

  public DeleteObjectRequest newDeleteRequest() {
    return new DeleteObjectRequest(bucket, destinationKey);
  }

  public AbortMultipartUploadRequest newAbortRequest() {
    return new AbortMultipartUploadRequest(bucket, destinationKey, uploadId);
  }

  /**
   * Build the destination path of the object.
   * @return the path
   * @throws IllegalStateException if the URI is invalid
   */
  public Path destinationPath() {
    Preconditions.checkState(StringUtils.isNotEmpty(uri), "Empty uri");

    try {
      return new Path(new URI(uri));
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Cannot parse URI " + uri);
    }
  }

  public int size() {
    return etags.size();
  }

  @Override
  public Iterator<String> iterator() {
    return etags.iterator();
  }

  /**
   * Load an instance from a file, then validate it.
   * @param fs filesystem
   * @param path path
   * @return the loaded instance
   * @throws IOException IO failure
   * @throws IllegalStateException if the data is invalid
   */
  public static SinglePendingCommit load(FileSystem fs, Path path)
      throws IOException, IllegalStateException {
    SinglePendingCommit instance = getSerializer().load(fs, path);
    instance.validate();
    instance.filename = path.toString();
    return instance;
  }

}
