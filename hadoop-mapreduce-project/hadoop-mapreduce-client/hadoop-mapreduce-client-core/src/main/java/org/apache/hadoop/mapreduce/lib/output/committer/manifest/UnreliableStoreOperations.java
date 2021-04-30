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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPERATION_TIMED_OUT;

/**
 * Wrap an existing StoreOperations implementation and fail on
 * specific paths.
 * This is for testing. It could be implemented via
 * Mockito 2 spy code but is not so that
 * 1. It can be backported to Hadoop versions using Mockito 1.x.
 * 2. It can be extended to use in production. This is why it is in
 * the production module -to allow for downstream test to adopt it.
 * 3. Given the failure modes are known, this will be easier to maintain
 * than any Mockito classes.
 */
@InterfaceStability.Unstable
public class UnreliableStoreOperations implements StoreOperations {

  private static final Logger LOG = LoggerFactory.getLogger(
      UnreliableStoreOperations.class);

  public static final String OPERATION_COULD_NOT_BE_COMPLETED
      = "Operation could not be completed within the specified time";

  public static final String SIMULATED_FAILURE = "Simulated failure";

  private final StoreOperations wrappedOperations;

  private final Set<Path> deletePathsToFail = new HashSet<>();

  private final Set<Path> deletePathsToTimeOut = new HashSet<>();

  private final Set<Path> renameSourceFilesToFail = new HashSet<>();

  private final Set<Path> renameDestDirsToFail = new HashSet<>();

  private final Set<Path> moveToTrashToFail = new HashSet<>();

  private int timeoutSleepTimeMillis;

  private boolean renameToFailWithException = true;

  public UnreliableStoreOperations(final StoreOperations wrappedOperations) {
    this.wrappedOperations = wrappedOperations;
  }


  /**
   * Reset everything.
   */
  public void reset() {
    deletePathsToFail.clear();
    deletePathsToTimeOut.clear();
    renameSourceFilesToFail.clear();
    renameDestDirsToFail.clear();
    moveToTrashToFail.clear();
    timeoutSleepTimeMillis = 0;
  }
  public int getTimeoutSleepTimeMillis() {
    return timeoutSleepTimeMillis;
  }

  public void setTimeoutSleepTimeMillis(final int timeoutSleepTimeMillis) {
    this.timeoutSleepTimeMillis = timeoutSleepTimeMillis;
  }

  public boolean isRenameToFailWithException() {
    return renameToFailWithException;
  }

  public void setRenameToFailWithException(final boolean renameToFailWithException) {
    this.renameToFailWithException = renameToFailWithException;
  }

  /**
   * Add a path to the list of delete paths to fail.
   * @param path path to add.
   */
  public void addDeletePathToFail(Path path) {
    deletePathsToFail.add(path);
  }

  /**
   * Add a path to the list of delete paths to time out.
   * @param path path to add.
   */
  public void addDeletePathToTimeOut(Path path) {
    deletePathsToTimeOut.add(path);
  }

  /**
   * Add a path to the list of rename source paths to fail.
   * @param path path to add.
   */
  public void addRenameSourceFilesToFail(Path path) {
    renameSourceFilesToFail.add(path);
  }

  /**
   * Add a path to the list of dest dirs to fail.
   * @param path path to add.
   */
  public void addRenameDestDirsFail(Path path) {
    renameDestDirsToFail.add(path);
  }

  /**
   * Add a path to the list of move to trash paths to fail.
   * @param path path to add.
   */
  public void addMoveToTrashToFail(Path path) {
    moveToTrashToFail.add(path);
  }

  /**
   * Raise an exception if the path is in the set of target paths.
   * @param operation operation which failed.
   * @param path path to check
   * @param paths paths to probe for {@code path} being in.
   * @throws IOException simulated failure
   */
  private void maybeRaiseIOE(String operation, Path path, Set<Path> paths)
      throws IOException {
    if (paths.contains(path)) {
      LOG.info("Simulating failure of {} with {}", operation, path);
      throw new PathIOException(path.toString(),
          SIMULATED_FAILURE + " of " + operation);
    }
  }

  /**
   * Time out if the path is in the list of timeout paths.
   * Will sleep first, to help simulate delays.
   * @param operation operation which failed.
   * @param path path to check
   * @param paths paths to probe for {@code path} being in.
   * @throws IOException simulated timeout
   */
  private void maybeTimeout(String operation, Path path, Set<Path> paths)
      throws IOException {
    if (paths.contains(path)) {
      LOG.info("Simulating timeout of {} with {}", operation, path);
      try {
        if (timeoutSleepTimeMillis > 0) {
          Thread.sleep(timeoutSleepTimeMillis);
        }
      } catch (InterruptedException e) {
        throw new InterruptedIOException(e.toString());
      }
      throw new PathIOException(path.toString(),
          "ErrorCode=" + OPERATION_TIMED_OUT
              + " ErrorMessage=" + OPERATION_COULD_NOT_BE_COMPLETED);
    }
  }

  @Override
  public FileStatus getFileStatus(final Path path) throws IOException {
    return wrappedOperations.getFileStatus(path);
  }

  @Override
  public boolean delete(final Path path, final boolean recursive)
      throws IOException {
    String op = "delete";
    maybeTimeout(op, path, deletePathsToTimeOut);
    maybeRaiseIOE(op, path, deletePathsToFail);
    return wrappedOperations.delete(path, recursive);
  }

  @Override
  public boolean mkdirs(final Path path) throws IOException {
    return wrappedOperations.mkdirs(path);
  }

  @Override
  public boolean renameFile(final Path source, final Path dest)
      throws IOException {
    String op = "rename";
    if (renameToFailWithException) {
      maybeRaiseIOE(op, source, renameSourceFilesToFail);
      maybeRaiseIOE(op, dest.getParent(), renameDestDirsToFail);
    } else {
      if (renameSourceFilesToFail.contains(source)
          || renameDestDirsToFail.contains(dest.getParent())) {
        LOG.info("Failing rename({}, {})", source, dest);
        return false;
      }
    }
    return wrappedOperations.renameFile(source, dest);
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(final Path path)
      throws IOException {
    return wrappedOperations.listStatusIterator(path);
  }

  @Override
  public TaskManifest loadTaskManifest(final FileStatus st) throws IOException {
    return wrappedOperations.loadTaskManifest(st);
  }

  @Override
  public void save(final AbstractManifestData manifestData,
      final Path path,
      final boolean overwrite) throws IOException {
    wrappedOperations.save(manifestData, path, overwrite);
  }

  @Override
  public boolean moveToTrash(final String jobId, final Path path) {
    if (moveToTrashToFail.contains(path)) {
      return false;
    }
    return wrappedOperations.moveToTrash(jobId, path);
  }

  @Override
  public void close() throws IOException {
    wrappedOperations.close();
  }

}
