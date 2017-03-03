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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.hadoop.fs.s3a.commit.CommitUtils.*;

/**
 * Adds the code needed for S3A integration.
 * It's pulled out to keep S3A FS class slightly less complex.
 * This class can be instantiated even when delayed commit is disabled;
 * in this case:
 * <ol>
 *   <li>{@link #isDelayedCompletePath(Path)} will always return false</li>
 *   <li>{@link #getTracker(Path, String)} will always return an instance
 *   of {@link DefaultPutTracker}.</li>
 * </ol>
 *
 *
 * always
 */
public class DelayedCommitFSIntegration {
  private static final Logger LOG =
      LoggerFactory.getLogger(DelayedCommitFSIntegration.class);
  private final S3AFileSystem owner;
  private final boolean delayedCommitEnabled;

  /**
   * Instantiate
   * @param owner pwner class
   * @param delayedCommitEnabled is delayed commit enabled.
   */
  public DelayedCommitFSIntegration(S3AFileSystem owner,
      boolean delayedCommitEnabled) {
    this.owner = owner;
    this.delayedCommitEnabled = delayedCommitEnabled;
  }

  /**
   * Given an (elements, key) pair, return the key of the final destination of
   * the PUT, that is: where the final path is expected to go?
   * @param elements path split to elements
   * @param key key
   * @return key for final put. If this is not a delayed complete operation, the
   * same as the key in.
   */
  public String keyOfFinalDestination(List<String> elements, String key) {
    if (isDelayedCommitPath(elements)) {
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
    if (isDelayedCommitPath(elements)) {
      final String destKey = keyOfFinalDestination(elements, key);
      String pendingKey = key + Constants.PENDING_SUFFIX;
      tracker = new DelayedCommitTracker(path, destKey, pendingKey,
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
    return delayedCommitEnabled ?
        CommitUtils.finalDestination(elements)
        : elements;
  }

  /**
   * Is delayed complete enabled?
   * @return true if delayed completion is turned on.
   */
  public boolean isDelayedCommitEnabled() {
    return delayedCommitEnabled;
  }

  /**
   * Predicate: is a path a delayed commit path?
   * True if delayed commit is enabled and the path contains the pending path
   * somewhere in it.
   * @param path path to examine
   * @return true if the path is or is under a pending directory
   */
  public boolean isDelayedCompletePath(Path path) {
    return isDelayedCommitPath(splitPathToElements(path));
  }

  private boolean isDelayedCommitPath(List<String> elements) {
    return delayedCommitEnabled && isPendingPath(elements);
  }

}
