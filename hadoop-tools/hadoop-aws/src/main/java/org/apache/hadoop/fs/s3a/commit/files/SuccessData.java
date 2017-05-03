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

package org.apache.hadoop.fs.s3a.commit.files;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.ValidationFailure;
import org.apache.hadoop.util.JsonSerDeser;

/**
 * Summary data saved into a {@code _SUCCESS} marker file.
 *
 * This provides an easy way to determine which committer was used
 * to commit work.
 * <ol>
 *   <li>File length == 0: classic {@code FileOutputCommitter}.</li>
 *   <li>Loadable as {@SuccessData}: committer in {@link #committer} field.</li>
 *   <li>Not loadable? Something else.</li>
 * </ol>
 *
 * This is an unstable structure intended for diagnostics and testing.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SuccessData extends PersistentCommitData {
  private static final Logger LOG = LoggerFactory.getLogger(
      SuccessData.class);
  /**
   * Serialization ID: {@value}.
   */
  private static final long serialVersionUID = 507133045258460084L;

  /** Timestamp of creation. */
  public long timestamp;

  /** Timestamp as date; no expectation of parseability. */
  public String date;

  /**
   * Host which created the file (implicitly: committed the work).
   */
  public String hostname;

  /**
   * Committer name.
   */
  public String committer;

  /**
   * Description text.
   */
  public String description;

  /**
   * Metrics.
   */
  public Map<String, Long> metrics = new HashMap<>();

  @Override
  public void validate() throws ValidationFailure {

  }

  @Override
  public byte[] toBytes() throws IOException {
    return serializer().toBytes(this);
  }

  @Override
  public void save(FileSystem fs, Path path, boolean overwrite)
      throws IOException {
    serializer().save(fs, path, this, overwrite);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "SuccessData{");
    sb.append("committer='").append(committer).append('\'');
    sb.append(", hostname='").append(hostname).append('\'');
    sb.append(", description='").append(description).append('\'');
    sb.append(", date='").append(date).append('\'');
    sb.append('}');
    return sb.toString();
  }

  /**
   * Load an instance from a file, then validate it.
   * @param fs filesystem
   * @param path path
   * @return the loaded instance
   * @throws IOException IO failure
   * @throws ValidationFailure if the data is invalid
   */
  public static SuccessData load(FileSystem fs, Path path)
      throws IOException {
    LOG.debug("Reading success data from {}", path);
    SuccessData instance = serializer().load(fs, path);
    instance.validate();
    return instance;
  }

  /**
   * Get a JSON serializer for this class.
   * @return a serializer.
   */
  private static JsonSerDeser<SuccessData> serializer() {
    return new JsonSerDeser<>(SuccessData.class, false, true);
  }

}
