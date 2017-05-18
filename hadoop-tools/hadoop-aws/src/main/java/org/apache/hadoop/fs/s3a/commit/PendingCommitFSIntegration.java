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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.s3a.commit.CommitUtils.*;

/**
 * Adds the code needed for S3A integration.
 * It's pulled out to keep S3A FS class slightly less complex.
 * This class can be instantiated even when pending commit is disabled;
 * in this case:
 * <ol>
 *   <li>{@link #isPendingCommitPath(Path)} will always return false.</li>
 *   <li>{@link #getTracker(Path, String)} will always return an instance
 *   of {@link DefaultPutTracker}.</li>
 * </ol>
 */
public class PendingCommitFSIntegration {
  private static final Logger LOG =
      LoggerFactory.getLogger(PendingCommitFSIntegration.class);
  private final S3AFileSystem owner;
  private final boolean pendingCommitEnabled;

  /**
   * Instantiate.
   * @param owner owner class
   * @param pendingCommitEnabled is pending commit enabled.
   */
  public PendingCommitFSIntegration(S3AFileSystem owner,
      boolean pendingCommitEnabled) {
    this.owner = owner;
    this.pendingCommitEnabled = pendingCommitEnabled;
  }

  /**
   * Given an (elements, key) pair, return the key of the final destination of
   * the PUT, that is: where the final path is expected to go?
   * @param elements path split to elements
   * @param key key
   * @return key for final put. If this is not a pending commit, the
   * same as the key in.
   */
  public String keyOfFinalDestination(List<String> elements, String key) {
    if (isPendingCommitPath(elements)) {
      return elementsToKey(finalDestination(elements));
    } else {
      return key;
    }
  }

  /**
   * Given a path and a key to that same path, get a tracker for it.
   * This specific tracker will be chosen based on whether or not
   * the path is a pending one.
   * @param path path of nominal write
   * @param key key of path of nominal write
   * @return the tracker for this operation.
   */
  public DefaultPutTracker getTracker(Path path, String key) {
    final List<String> elements = splitPathToElements(path);
    DefaultPutTracker tracker;
    if (isPendingCommitPath(elements)) {
      final String destKey = keyOfFinalDestination(elements, key);
      String pendingKey = key + CommitConstants.PENDING_SUFFIX;
      tracker = new PendingCommitTracker(path,
          owner.getBucket(),
          destKey, pendingKey,
          owner.createWriteOperationHelper(pendingKey));
    } else {
      // standard multipart tracking
      tracker = new DefaultPutTracker(key);
    }
    LOG.debug("Created {}", tracker);
    return tracker;
  }

  /**
   * This performs the calculation of the final destination of a set
   * of elements.
   *
   * @param elements original (do not edit after this call)
   * @return a list of elements, possibly empty
   */
  private List<String> finalDestination(List<String> elements) {
    return pendingCommitEnabled ?
        CommitUtils.finalDestination(elements)
        : elements;
  }

  /**
   * Is pending commit enabled?
   * @return true if pending commit is turned on.
   */
  public boolean isPendingCommitEnabled() {
    return pendingCommitEnabled;
  }

  /**
   * Predicate: is a path a pending commit path?
   * @param path path to examine
   * @return true if the path is or is under a pending directory
   */
  public boolean isPendingCommitPath(Path path) {
    return isPendingCommitPath(splitPathToElements(path));
  }

  /**
   * Is this path a pending commit path in this filesystem?
   * True if pending commit is enabled, the path is magic
   * and the path is not actually a pending file.
   * @param elements element list
   * @return true if the path is for pending commits
   */
  private boolean isPendingCommitPath(List<String> elements) {
    return pendingCommitEnabled && isMagicPath(elements) &&
        !isPendingFile(elements);
  }

  /**
   * Is this a pending file?
   * @param elements path element list
   * @return true if this file is one of the pending files.
   */
  private boolean isPendingFile(List<String> elements) {
    String last = elements.get(elements.size() - 1);
    return last.endsWith(CommitConstants.PENDING_SUFFIX)
        || last.endsWith(CommitConstants.PENDINGSET_SUFFIX);
  }

}
