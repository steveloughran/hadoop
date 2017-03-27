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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.JsonSerDeser;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.fs.s3a.commit.CommitUtils.validateCollectionClass;

/**
 * Persistent format for multiple pending commits.
 * Contains 0 or more {@link SinglePendingCommit} entries; validation logic
 * checks those values on load.
 */
public class MultiplePendingCommits extends PersistentCommitData {

  public MultiplePendingCommits() {
    this(0);
  }

  public MultiplePendingCommits(int size) {
    commits = new ArrayList<>(size);
  }

  private static JsonSerDeser<MultiplePendingCommits> serializer
      = new JsonSerDeser<>(MultiplePendingCommits.class, false, true);

  /**
   * Supported version value: {@value}.
   * If this is changed the value of {@link #serialVersionUID} will change,
   * to avoid deserialization problems.
   */
  public static final int VERSION = 1;

  /**
   * Serialization ID: {@value}.
   */
  private static final long serialVersionUID = 0x11000 + VERSION;

  /** Version marker. */
  public int version = VERSION;

  /**
   * Commit list.
   */
  public List<SinglePendingCommit> commits;


  public void add(SinglePendingCommit commit) {
    commits.add(commit);
  }
  /**
   * Any custom extra data committer subclasses may choose to add.
   */
  public Map<String, String> extraData = new HashMap<>(0);

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
    Preconditions.checkState(version == VERSION, "Wrong version: %s", version);
    if (extraData != null) {
      validateCollectionClass(extraData.keySet(), String.class);
      validateCollectionClass(extraData.values(), String.class);
    }
    Set<String> destinations = new HashSet<>(commits.size());
    validateCollectionClass(commits, SinglePendingCommit.class);
    for (SinglePendingCommit c : commits) {
      c.validate();
      Preconditions.checkArgument(!destinations.contains(c.destinationKey),
          "Destination %s is written to by more than one pending commit",
          c.destinationKey);
      destinations.add(c.destinationKey);
    }

  }

  @Override
  public byte[] toBytes() throws IOException {
    return serializer.toBytes(this);
  }

  /**
   * Number of commits
   * @return the number of commits in this structure.
   */

  public int size() {
    return commits.size();
  }

  @Override
  public void save(FileSystem fs, Path path, boolean overwrite)
      throws IOException {
    serializer.save(fs, path, this, overwrite);
  }

  /**
   * Get the singleton JSON serializer for this class.
   * @return the serializer.
   */
  public static JsonSerDeser<MultiplePendingCommits> getSerializer() {
    return serializer;
  }

  public static MultiplePendingCommits load(FileSystem fs, Path path)
      throws IOException {
    return MultiplePendingCommits.getSerializer().load(fs, path);
  }
}
