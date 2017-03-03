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

import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.JsonSerDeser;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;

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
public final class PersistentCommitData implements Serializable {

  private static JsonSerDeser<PersistentCommitData> serializer
      = new JsonSerDeser<>(PersistentCommitData.class, false, true);

  /**
   * Supported version value: {@value}.
   * If this is changed the valure of {@link #serialVersionUID} will change,
   * to avoid deserialization problems.
   */
  public static final int VERSION = 1;

  /**
   * Serialization ID: {@value}.
   */
  private static final long serialVersionUID = 0x10000 + VERSION;

  /** Version marker. */
  public int version = VERSION;

  /** Path URI. */
  public String uri = "";

  /** Destination key in the bucket. */
  public String destinationKey;

  /** ID of the upload. */
  public String uploadId;

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
  public String notes = "";

  /** Ordered list of etags. */
  public List<String> etags;

  /** Destination file size. */
  public long size;

  public PersistentCommitData() {
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
   * Validate the data: those fields which must be non empty, must be set.
   * @throws IllegalStateException if the data is invalid
   */
  public void validate() {
    Preconditions.checkState(version == VERSION, "Wrong version: {}", version);
    Preconditions.checkState(StringUtils.isNotEmpty(destinationKey),
        "Empty destination");
    Preconditions.checkState(StringUtils.isNotEmpty(uploadId),
        "Empty uploadId");
    Preconditions.checkState(size >= 0, "Invalid size: " + size);
    Preconditions.checkState(StringUtils.isNotEmpty(uri), "Empty uri");
    Preconditions.checkState(etags != null, "No etag list");
    Preconditions.checkState(!etags.isEmpty(), "Empty etag list");
    for (String etag : etags) {
      Preconditions.checkState(StringUtils.isNotEmpty(etag), "Empty etag");
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
    sb.append(", notes='").append(notes).append('\'');
    if (etags != null) {
      sb.append('[');
      sb.append(join(",", etags));
      sb.append(']');
    } else {
      sb.append(", etags=null");
    }
    sb.append('}');
    return sb.toString();
  }

  /**
   * Get the singleton JSON serializer for this class.
   * @return the serializer.
   */
  public static JsonSerDeser<PersistentCommitData> getSerializer() {
    return serializer;
  }
}
