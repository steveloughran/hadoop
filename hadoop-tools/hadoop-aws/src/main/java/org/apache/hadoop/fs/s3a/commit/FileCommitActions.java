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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;

/**
 * The implementation of the various actions a committer needs.
 * This doesn't implement the protocol/binding to a specific execution engine,
 * just the operations needed to to build one.
 */
public class FileCommitActions {
  private static final Logger LOG = LoggerFactory.getLogger(
      FileCommitActions.class);

  private final S3AFileSystem fs;

  public FileCommitActions(S3AFileSystem fs) {
    Preconditions.checkArgument(fs != null, "null fs");
    this.fs = fs;
  }

  /**
   * Commit a pending file, then delete the data.
   * @param pendingFile path to the pending data
   * @return the outcome
   */
  public CommitFileOutcome commitPendingFile(Path pendingFile)
      throws IOException {
    Preconditions.checkArgument(pendingFile != null, "null pendingFile");
    CommitFileOutcome outcome;
    String destKey = null;
    // really read it in and parse
    try {
      SinglePendingCommit persisted = SinglePendingCommit.getSerializer()
          .load(fs, pendingFile);
      persisted.validate();
      destKey = persisted.destinationKey;
      LOG.info("Committing to key {} defined in {}",
          destKey, pendingFile);
      S3AFileSystem.WriteOperationHelper writer
          = fs.createWriteOperationHelper(destKey);
      writer.finalizeMultipartCommit(destKey, persisted.uploadId,
          CommitUtils.toPartEtags(persisted.etags),
          persisted.size);
      LOG.debug("Successfull commit");
      // now do a low level get to verify it is there
      Path destPath = fs.keyToQualifiedPath(destKey);
      FileStatus status = fs.getFileStatus(destPath);
      LOG.debug("Destination entry: {}", status);
      outcome = commitSuccess(pendingFile, destKey);
    } catch (IOException e) {
      String msg = String.format("Failed to commit upload against %s," +
          " described in %s: %s", destKey, pendingFile, e);
      LOG.warn(msg, e);
      outcome = commitFailure(pendingFile, destKey, e);
    } catch (Exception e) {
      String msg = String.format("Failed to commit upload against %s," +
          " described in %s: %s", destKey, pendingFile, e);
      LOG.warn(msg, e);
      outcome = commitFailure(pendingFile, destKey,
          new PathCommitException(pendingFile.toString(), msg,
              e));
    } finally {
      LOG.debug("Deleting file {}", pendingFile);
      deleteQuietly(fs, pendingFile, false);
    }
    LOG.debug("Commit outcome: {}", outcome);
    return outcome;
  }

  /**
   * Commit all pending files.
   * @param pendingDir directory of pending operations
   * @param recursive recurse?
   * @return the outcome of all the operations
   * @throws IOException if there is a problem listing the path.
   */
  public CommitAllFilesOutcome commitAllPendingFilesInPath(Path pendingDir,
      boolean recursive)
      throws IOException, FileNotFoundException {
    Preconditions.checkArgument(pendingDir != null, "null pendingDir");
    final CommitAllFilesOutcome outcome = new CommitAllFilesOutcome();
    FileStatus fileStatus = fs.getFileStatus(pendingDir);
    if (!fileStatus.isDirectory()) {
      throw new PathCommitException(pendingDir,
          "Not a directory : " + fileStatus);
    }
    RemoteIterator<LocatedFileStatus> pendingFiles
        = fs.listFiles(pendingDir, recursive);
    if (!pendingFiles.hasNext()) {
      LOG.info("No files to commit under {}", pendingDir);
    }
    while (pendingFiles.hasNext()) {
      LocatedFileStatus next = pendingFiles.next();
      Path pending = next.getPath();
      if (pending.getName().endsWith(PENDING_SUFFIX)) {
        outcome.add(commitPendingFile(pending));
      }
    }
    LOG.info("Committed operations: {}", outcome);
    return outcome;
  }

  /**
   * Abort an pending file commit.
   * This operation is designed to always
   * succeed; failures are caught and logged.
   * @param pendingFile path
   * @param ignoreMissingFile treat FNFEs as ignored.
   * @return the outcome
   */
  public CommitFileOutcome abortPendingFile(Path pendingFile,
      boolean ignoreMissingFile) {
    CommitFileOutcome outcome;
    String destKey = null;
    try {
      // really read it in and parse
      SinglePendingCommit persisted = SinglePendingCommit.getSerializer()
          .load(fs, pendingFile);
      persisted.validate();
      destKey = persisted.destinationKey;
      LOG.info("Aborting commit to file {} defined in {}",
          destKey, pendingFile);
      S3AFileSystem.WriteOperationHelper writer
          = fs.createWriteOperationHelper(destKey);
      writer.abortMultipartCommit(destKey, persisted.uploadId);
      outcome = commitSuccess(pendingFile, destKey);
    } catch (FileNotFoundException e) {
      // file isn't found, log
      LOG.info("File {} not found; no operation to abort", pendingFile);
      if (ignoreMissingFile) {
        LOG.debug("Ignoring missing file; marking as success");
        outcome = commitSuccess(pendingFile, destKey);
      } else {
        outcome = commitFailure(pendingFile, destKey, e);
      }
    } catch (IllegalArgumentException | IllegalStateException e) {
      String msg = String.format("Failed to abort upload against %s," +
          " described in %s: %s", destKey, pendingFile, e);
      LOG.warn(msg, e);
      outcome = commitFailure(pendingFile, destKey,
          new PathCommitException(pendingFile.toString(), msg, e));
    } catch (IOException e) {
      LOG.warn("Failed to abort upload against {}," +
          " described in {}", destKey, pendingFile, e);
      outcome = commitFailure(pendingFile, destKey, e);
    } finally {
      deleteQuietly(fs, pendingFile, false);
    }
    return outcome;
  }

  public static CommitFileOutcome commitSuccess(Path pendingFile,
      String destKey) {
    return new CommitFileOutcome(pendingFile, destKey);
  }

  public static CommitFileOutcome commitFailure(Path pendingFile,
      String destKey, IOException e) {
    return new CommitFileOutcome(pendingFile, destKey, e);
  }

  /**
   * Enumerate all pending files in a dir/tree, abort.
   * @param pendingDir directory of pending operations
   * @param recursive recurse?
   * @return the outcome of all the abort operations
   * @throws IOException if there is a problem listing the path.
   */
  public CommitAllFilesOutcome abortAllPendingFilesInPath(Path pendingDir,
      boolean recursive)
      throws IOException {
    Preconditions.checkArgument(pendingDir != null, "null pendingDir");
    CommitAllFilesOutcome outcome = new CommitAllFilesOutcome();
    RemoteIterator<LocatedFileStatus> pendingFiles;
    try {
      pendingFiles = fs.listFiles(pendingDir, recursive);
    } catch (FileNotFoundException e) {
      LOG.info("No directory to abort {}", pendingDir);
      return outcome;
    }
    if (!pendingFiles.hasNext()) {
      LOG.info("No files to abort under {}", pendingDir);
    }
    while (pendingFiles.hasNext()) {
      LocatedFileStatus next = pendingFiles.next();
      Path pending = next.getPath();
      if (pending.getName().endsWith(PENDING_SUFFIX)) {
        outcome.add(abortPendingFile(pending, true));
      }
    }
    LOG.info("aborted operations: {}", outcome);
    return outcome;
  }

  /**
   * Abort all pending uploads to the destination FS under a path.
   * @param dest destination path
   * @return a count of the number of uploads aborted.
   * @throws IOException IO failure
   */
  public int abortPendingUploadsUnderDestination(Path dest) throws IOException {
    String destKey = fs.pathToKey(dest);
    S3AFileSystem.WriteOperationHelper writer
        = fs.createWriteOperationHelper(destKey);
    return writer.abortMultipartUploadsUnderPath(destKey);
  }

  /**
   * Touch the success marker. This will overwrite it if it is already there.
   * @param outputPath output directory
   * @throws IOException IO problem
   */
  public void touchSuccessMarker(Path outputPath) throws IOException {
    Preconditions.checkArgument(outputPath != null, "null outputPath");

    Path markerPath = new Path(outputPath, SUCCESS_FILE_NAME);
    LOG.debug("Touching success marker for job {}", markerPath);
    fs.create(markerPath, true).close();
  }

  /**
   * Outcome of a commit or abort operation, lists all successes and failures.
   */
  public static class CommitAllFilesOutcome {
    private final List<CommitFileOutcome> succeeded = new ArrayList<>();
    private final List<CommitFileOutcome> failed = new ArrayList<>();

    /**
     * Get the list of succeeded operations.
     * @return a possibly empty list.
     */
    public List<CommitFileOutcome> getSucceeded() {
      return succeeded;
    }

    /**
     * Get the list of failed operations.
     * @return a possibly empty list.
     */
    public List<CommitFileOutcome> getFailed() {
      return failed;
    }

    /**
     * Add a success.
     * @param pending pending path
     * @param destination destination path
     */
    public void success(Path pending, String destination) {
      succeeded.add(commitSuccess(pending, destination));
    }

    /**
     * Add a failure.
     * @param pending pending path
     * @param exception the exception causing the failure
     */
    public void failure(Path pending, String destination,
        IOException exception) {
      failed.add(commitFailure(pending, destination, exception));
    }

    /**
     * Get the total size of the outcome list.
     * @return the size of the list
     */
    public int size() {
      return succeeded.size() + failed.size();
    }

    /**
     * Add an outcome, choose the destination list from its success flag.
     * @param outcome outcome to add.
     */
    public void add(CommitFileOutcome outcome) {
      if (outcome.isSucceeded()) {
        succeeded.add(outcome);
      } else {
        failed.add(outcome);
      }
    }

    /**
     * Rethrow the exception in the first failure entry.
     * @throws IOException the first exception caught.
     */
    public void maybeRethrow() throws IOException {
      if (!failed.isEmpty()) {
        failed.get(0).maybeRethrow();
      }
    }

    /**
     * Get the first exception if there was one in the first failure.
     * This is the same exception which {@link #maybeRethrow()} will throw.
     * @return an exception or null.
     */
    public IOException getFirstException() {
      return !failed.isEmpty() ? failed.get(0).getException() : null;
    }

    @Override
    public String toString() {
      return String.format("successes=%d failures=%d, total=%d",
          succeeded.size(), failed.size(), size());
    }
  }


  /**
   * Outcome of a commit to a single file.
   */
  public static class CommitFileOutcome {
    private final boolean succeeded;
    private final Path pendingFile;
    private final String destination;
    private final IOException exception;

    /**
     * Success outcome.
     * @param pendingFile pending file
     * @param destination destination of commit
     */
    public CommitFileOutcome(Path pendingFile, String destination) {
      this.succeeded = true;
      this.destination = destination;
      this.pendingFile = pendingFile;
      this.exception = null;
    }

    /**
     * Failure outcome.
     * @param pendingFile pending file
     * @param exception failure cause
     */
    public CommitFileOutcome(Path pendingFile, IOException exception) {
      this(pendingFile, null, exception);
    }

    /**
     * Failure outcome.
     * @param pendingFile pending file
     * @param destination destination of commit
     * @param exception failure cause
     */
    public CommitFileOutcome(Path pendingFile,
        String destination,
        IOException exception) {
      this.succeeded = exception == null;
      this.destination = destination;
      this.pendingFile = pendingFile;
      this.exception = exception;
    }

    public boolean isSucceeded() {
      return succeeded;
    }

    public Path getPendingFile() {
      return pendingFile;
    }

    public IOException getException() {
      return exception;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "CommitFileOutcome{");
      sb.append(succeeded ? "success" : "failure");
      sb.append(", destination=").append(destination);
      sb.append(", pendingFile=").append(pendingFile);
      if (!succeeded) {
        sb.append(", exception=").append(exception);
      }
      sb.append('}');
      return sb.toString();
    }

    /**
     * Rethrow any exception which was in the outcome.
     * @throws IOException the exception field, if non-null.
     */
    public void maybeRethrow() throws IOException {
      if (exception != null) {
        throw exception;
      }
    }
  }

}
