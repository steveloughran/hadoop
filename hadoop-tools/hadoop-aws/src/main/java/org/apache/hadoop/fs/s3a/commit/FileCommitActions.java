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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.apache.hadoop.fs.s3a.commit.files.MultiplePendingCommits;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.fs.s3a.commit.files.SuccessData;
import org.apache.hadoop.fs.s3a.commit.magic.MagicCommitterConstants;

import static org.apache.hadoop.fs.s3a.S3AUtils.deleteQuietly;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.SUCCESS_FILE_NAME;

/**
 * The implementation of the various actions a committer needs.
 * This doesn't implement the protocol/binding to a specific execution engine,
 * just the operations needed to to build one.
 */
public class FileCommitActions {
  private static final Logger LOG = LoggerFactory.getLogger(
      FileCommitActions.class);

  private final S3AFileSystem fs;

  /**
   * Instantiate.
   * @param fs FS to bind to
   */
  public FileCommitActions(S3AFileSystem fs) {
    Preconditions.checkArgument(fs != null, "null fs");
    this.fs = fs;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "FileCommitActions{");
    sb.append("fs=").append(fs.getUri());
    sb.append('}');
    return sb.toString();
  }

  /**
   * Commit a pending file, then delete the data.
   * @param pendingFile path to the pending data
   * @return the outcome
   */
  public CommitFileOutcome commitPendingFile(Path pendingFile)
      throws IOException {
    Preconditions.checkArgument(pendingFile != null, "null pendingFile");
    // really read it in and parse
    try {
      SinglePendingCommit commit = SinglePendingCommit.load(fs, pendingFile);
      CommitFileOutcome outcome = commit(commit, pendingFile.toString());
      LOG.debug("Commit outcome: {}", outcome);
      return outcome;
    } finally {
      LOG.debug("Deleting file {}", pendingFile);
      deleteQuietly(fs, pendingFile, false);
    }
  }

  /**
   * Commit the operation, throwing an exception on any failure
   * @param commit commit to execute
   * @throws IOException on a failure
   */
  public void commitOrFail(SinglePendingCommit commit) throws IOException {
    commit(commit, commit.filename).maybeRethrow();
  }

  /**
   * Commit a single pending commit; exceptions are caught
   * and converted to an outcome.
   * @param commit entry to commit
   * @param origin origin path/string for outcome text
   * @return the outcome
   */
  public CommitFileOutcome commit(SinglePendingCommit commit, String origin) {
    LOG.debug("Committing single commit {}", commit);
    CommitFileOutcome outcome;
    String destKey = "unknown destination";
    try {
      commit.validate();
      destKey = commit.destinationKey;
      // finalize the commit
      writer(destKey).completeMultipartCommit(destKey,
          commit.uploadId,
          CommitUtils.toPartEtags(commit.etags),
          commit.size);
      LOG.debug("Successfull commit");
      outcome = commitSuccess(origin, destKey);
    } catch (IOException e) {
      String msg = String.format("Failed to commit upload against %s: %s",
          destKey, e);
      LOG.warn(msg, e);
      outcome = commitFailure(origin, destKey, e);
    } catch (Exception e) {
      String msg = String.format("Failed to commit upload against %s," +
          " described in %s: %s", destKey, origin, e);
      LOG.warn(msg, e);
      outcome = commitFailure(origin, destKey,
          new PathCommitException(origin, msg, e));
    }
    LOG.debug("Commit outcome: {}", outcome);
    return outcome;
  }

  /**
   * Create a new {@link WriteOperationHelper} for working with the destination.
   * @param destKey destination key
   * @return a new instance
   */
  private WriteOperationHelper writer(String destKey) {
    return fs.createWriteOperationHelper(destKey);
  }

  /**
   * Verify that the path at the end of a commit exists. This does
   * not validate the size.
   * @param commit commit to verify
   * @throws FileNotFoundException dest doesn't exist
   * @throws ValidationFailure commit arg is invalid
   * @throws IOException invalid commit, IO failure
   */
  public void verifyCommitExists(SinglePendingCommit commit)
      throws FileNotFoundException, ValidationFailure, IOException {
    commit.validate();
    // this will force an existence check
    Path path = fs.keyToQualifiedPath(commit.destinationKey);
    FileStatus status = fs.getFileStatus(
        path);
    LOG.debug("Destination entry: {}", status);
    if (!status.isFile()) {
      throw new PathCommitException(path, "Not a file: " + status);
    }
  }

  /**
   * Commit all single pending files in a directory tree.
   * @param pendingDir directory of pending operations
   * @param recursive recurse?
   * @return the outcome of all the operations
   * @throws IOException if there is a problem listing the path.
   */
  public CommitAllFilesOutcome commitSinglePendingCommitFiles(Path pendingDir,
      boolean recursive) throws IOException {
    Preconditions.checkArgument(pendingDir != null, "null pendingDir");
    Pair<MultiplePendingCommits, List<LocatedFileStatus>> results
        = loadSinglePendingCommits(pendingDir, recursive);
    final CommitAllFilesOutcome outcome = new CommitAllFilesOutcome();
    for (SinglePendingCommit singlePendingCommit : results.first().commits) {
      CommitFileOutcome commit = commit(singlePendingCommit,
          singlePendingCommit.filename);
      outcome.add(commit);
    }
    LOG.info("Committed operations: {}", outcome);
    return outcome;
  }

  /**
   * Locate all files with the pending suffix under a directory.
   * @param pendingDir directory
   * @param recursive recursive listing?
   * @return the list of all located entries
   * @throws IOException if there is a problem listing the path.
   */
  public List<LocatedFileStatus> locateAllSinglePendingCommits(
      Path pendingDir,
      boolean recursive) throws IOException {
    final List<LocatedFileStatus> result = new ArrayList<>();
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
      if (next.getPath().getName().endsWith(
          MagicCommitterConstants.PENDING_SUFFIX) && next.isFile()) {
        result.add(next);
      }
    }
    return result;
  }

  /**
   * Load all single pending commits in the directory.
   * All load failures are logged and then added to list of files which would
   * not load.
   * @param pendingDir directory containing commits
   * @param recursive do a recursive scan?
   * @return tuple of loaded entries and those pending files which would
   * not load/validate.
   * @throws IOException on a failure to list the files.
   */
  public Pair<MultiplePendingCommits, List<LocatedFileStatus>>
      loadSinglePendingCommits(Path pendingDir,
        boolean recursive) throws IOException {
    List<LocatedFileStatus> statusList = locateAllSinglePendingCommits(
        pendingDir, recursive);
    MultiplePendingCommits commits = new MultiplePendingCommits(
        statusList.size());
    List<LocatedFileStatus> failures = new ArrayList<>(1);
    for (LocatedFileStatus status : statusList) {
      try {
        commits.add(SinglePendingCommit.load(fs, status.getPath()));
      } catch (IOException e) {
        LOG.warn("Failed to load commit file {}", status.getPath(), e);
        failures.add(status);
      }
    }
    return Pair.of(commits, failures);
  }

  /**
   * Abort an pending file commit.
   * This operation is designed to always
   * succeed; failures are caught and logged.
   * @param pendingFile path
   * @return the outcome
   */
  public CommitFileOutcome abortSinglePendingCommitFile(Path pendingFile) {
    CommitFileOutcome outcome;
    try {
      // read it in and abort
      outcome = abort(SinglePendingCommit.load(fs, pendingFile));
    } catch (IOException e) {
      // abort failed to load/validate
      String origin = pendingFile.toString();
      outcome = new CommitFileOutcome(CommitOutcomes.ABORT_FAILED,
          origin, null, e);
    } finally {
      deleteQuietly(fs, pendingFile, false);
    }
    return outcome;
  }

  /**
   * Abort a pending commit, returning an outcome of type
   * {@link CommitOutcomes#ABORTED} describing the operation.
   * Failures are caught and result in an outcome of the type
   * {@link CommitOutcomes#ABORT_FAILED}
   * @param commit commit data
   * @return the outcome
   */
  public CommitFileOutcome abort(SinglePendingCommit commit) {
    CommitFileOutcome outcome;
    String destKey = commit.destinationKey;
    String origin = commit.filename;
    try {
      abortMultipartCommit(commit);
      outcome = new CommitFileOutcome(CommitOutcomes.ABORTED,
          origin, destKey, null);
    } catch (IOException | IllegalArgumentException e) {
      // download to an abort + exception
      LOG.warn("Failed to abort upload against {}," +
          " described in {}",
          destKey, origin, e);
      outcome = new CommitFileOutcome(CommitOutcomes.ABORT_FAILED,
          origin,
          destKey,
          e instanceof IOException ? (IOException) e
              : new PathCommitException(destKey, e.toString(), e));
    }
    return outcome;
  }

  /**
   * Abort the multipart commit supplied. This is the lower level operation
   * which doesn't generate an outcome, instead raising an exception.
   * @param commit pending commit to abort
   * @throws IOException on any failure
   */
  public void abortMultipartCommit(SinglePendingCommit commit)
      throws IOException {
    String destKey = commit.destinationKey;
    String origin = commit.filename !=null ?
        (" defined in " + commit.filename)
        : "";
    String uploadId = commit.uploadId;
    LOG.info("Aborting commit to object {}{}",
        destKey, origin);
    abortMultipartCommit(destKey, uploadId);
  }

  /**
   * Create an {@code AbortMultipartUpload} request and POST it
   * to S3.
   * @param destKey destination key
   * @param uploadId upload to cancel
   * @throws IOException on any failure
   */
  public void abortMultipartCommit(String destKey, String uploadId)
      throws IOException {
    writer(destKey).abortMultipartCommit(destKey, uploadId);
  }

  public static CommitFileOutcome commitSuccess(String origin,
      String destKey) {
    return new CommitFileOutcome(origin, destKey);
  }

  public static CommitFileOutcome commitFailure(String origin,
      String destKey, IOException e) {
    return new CommitFileOutcome(origin, destKey, e);
  }

  /**
   * Enumerate all pending files in a dir/tree, abort.
   * @param pendingDir directory of pending operations
   * @param recursive recurse?
   * @return the outcome of all the abort operations
   * @throws IOException if there is a problem listing the path.
   */
  public CommitAllFilesOutcome abortAllSinglePendingCommits(Path pendingDir,
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
      LOG.debug("No files to abort under {}", pendingDir);
    }
    while (pendingFiles.hasNext()) {
      LocatedFileStatus next = pendingFiles.next();
      Path pending = next.getPath();
      if (pending.getName().endsWith(MagicCommitterConstants.PENDING_SUFFIX)) {
        outcome.add(abortSinglePendingCommitFile(pending));
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
  public int abortPendingUploadsUnderPath(Path dest) throws IOException {
    String destKey = fs.pathToKey(dest);
    return writer(destKey).abortMultipartUploadsUnderPath(destKey);
  }

  /**
   * Save the success data to the {@code _SUCCESS} file.
   * @param outputPath output directory
   * @param successData success data to save.
   * @throws IOException IO problem
   */
  public void createSuccessMarker(Path outputPath, SuccessData successData)
      throws IOException {
    Preconditions.checkArgument(outputPath != null, "null outputPath");

    Path markerPath = new Path(outputPath, SUCCESS_FILE_NAME);
    LOG.debug("Touching success marker for job {}", markerPath);
    successData.save(fs, markerPath, true);
  }

  /**
   * Revert a pending commit by deleting the destination.
   * @param commit pending commit
   * @throws IOException failure
   */
  public void revertCommit(SinglePendingCommit commit) throws IOException {
    LOG.debug("Revert {}", commit);
    writer(commit.destinationKey)
        .revertCommit(commit.destinationKey);
  }


  /**
   * Upload all the data in the local file, returning the information
   * needed to commit the work.
   * @param localFile local file (be  a file)
   * @param partition partition/subdir. Not used
   * @param bucket dest bucket
   * @param key dest key
   * @param destURI destination
   * @param uploadPartSize size of upload  @return a pending upload entry
   * @return the commit data
   * @throws IOException failure
   */
  public SinglePendingCommit uploadFileToPendingCommit(File localFile,
      String partition,
      String bucket,
      String key,
      String destURI,
      long uploadPartSize)
      throws IOException {

    LOG.debug("Initiating multipart upload from {} to s3a://{}/{}" +
            "" +
            " partition={} partSize={}",
        localFile, bucket, key, partition, uploadPartSize);
    if (!localFile.exists()) {
      throw new FileNotFoundException(localFile.toString());
    }
    if (!localFile.isFile()) {
      throw new IOException("Not a file: " + localFile);
    }
    WriteOperationHelper writer = writer(key);
    String uploadId = null;

    boolean threw = true;
    try {
      uploadId = writer.initiateMultiPartUpload();
      long length = localFile.length();

      SinglePendingCommit commitData = new SinglePendingCommit();
      commitData.destinationKey = key;
      commitData.bucket = bucket;
      commitData.touch(System.currentTimeMillis());
      commitData.uploadId = uploadId;
      commitData.uri = destURI;
      commitData.text = partition != null ? "partition: " + partition : "";
      commitData.size = length;

      long offset = 0;
      long numParts = (length / uploadPartSize +
                           ((length % uploadPartSize) > 0 ? 1 : 0));

      List<PartETag> parts = new ArrayList<>((int) numParts);

      LOG.debug("File size is {}, number of parts to upload = {}",
          length, numParts);
      for (int partNumber = 1; partNumber <= numParts; partNumber += 1) {
        long size = Math.min(length - offset, uploadPartSize);
        UploadPartRequest part;
        part = writer.newUploadPartRequest(
            uploadId,
            partNumber,
            (int) size,
            null,
            localFile,
            offset
        );
        part.setLastPart(partNumber == numParts);

        UploadPartResult partResult = writer.uploadPart(part);
        offset += uploadPartSize;
        parts.add(partResult.getPartETag());
      }

      commitData.bindCommitData(parts);
      threw = false;
      return commitData;
    } finally {
      if (threw && uploadId != null) {
        try {
          abortMultipartCommit(key, uploadId);
        } catch (IOException e) {
          LOG.error("Failed to abort upload {} to {}", uploadId, key, e);
        }
      }
    }
  }

  /**
   * Outcome of a commit or abort operation, lists all successes and failures.
   */
  public static class CommitAllFilesOutcome {
    private final List<CommitFileOutcome> outcomes = new ArrayList<>();
    private final List<CommitFileOutcome> succeeded = new ArrayList<>();

    /**
     * Get the list of succeeded operations.
     * @return a possibly empty list.
     */
    public List<CommitFileOutcome> getSucceeded() {
      return succeeded;
    }

    /**
     * Add a success.
     * @param pending pending path
     * @param destination destination path
     */
    public void success(Path pending, String destination) {
      outcomes.add(commitSuccess(pending.toString(), destination));
    }

    /**
     * Add a failure.
     * @param pending pending path
     * @param exception the exception causing the failure
     */
    public void failure(Path pending, String destination,
        IOException exception) {
      outcomes.add(commitFailure(pending.toString(), destination, exception));
    }

    /**
     * Select all outcomes of a specific type
     * @param expected expected outcome
     * @return an iterator over all values matching the expected type.
     */
    public Iterable<CommitFileOutcome> select(final CommitOutcomes expected) {
      return Iterables.filter(outcomes,
          input -> input.outcome == expected);
    }

    /**
     * Predicate: does the outcome list include an entry of the given type
     * @param expected expected value
     * @return true if such an outcome exists
     */
    public boolean hasOutcome(final CommitOutcomes expected) {
      Iterator<CommitFileOutcome> iterator = select(expected).iterator();
      return iterator.hasNext();
    }

    /**
     * Get the total size of the outcome list.
     * @return the size of the list
     */
    public int size() {
      return outcomes.size();
    }

    /**
     * Add an outcome, choose the destination list from its success flag.
     * @param outcome outcome to add.
     */
    public void add(CommitFileOutcome outcome) {
      outcomes.add(outcome);
    }

    public CommitFileOutcome firstOutcome(final CommitOutcomes expected) {
      Iterator<CommitFileOutcome> iterator = select(expected).iterator();
      if (iterator.hasNext()) {
        return iterator.next();
      } else {
        return null;
      }
    }

    /**
     * Rethrow the exception in the first failure entry.
     * @throws IOException the first exception caught.
     */
    public void maybeRethrow() throws IOException {
      CommitFileOutcome failure = firstOutcome(CommitOutcomes.FAILED);
      if (failure != null) {
        throw failure.getException();
      }
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "CommitAllFilesOutcome{");
      sb.append("outcome count=").append(outcomes.size());
      sb.append('}');
      return sb.toString();
    }
  }


  /**
   * Possible outcomes of a commit operation.
   */
  enum CommitOutcomes {
    SUCCEEDED,
    FAILED,
    ABORTED,
    ABORT_FAILED,
    REVERTED
  }

  /**
   * Outcome of a commit to a single file.
   */
  public static class CommitFileOutcome {
    private final CommitOutcomes outcome;
    private final String origin;
    private final String destination;
    private final IOException exception;

    /**
     * Success outcome.
     * @param origin pending file
     * @param destination destination of commit
     */
    public CommitFileOutcome(String origin, String destination) {
      this(CommitOutcomes.SUCCEEDED, origin, destination, null);
    }

    /**
     * Failure outcome.
     * @param origin pending file
     * @param destination destination of commit
     * @param exception failure cause
     */
    public CommitFileOutcome(String origin,
        String destination,
        IOException exception) {
      this(exception == null ?
              CommitOutcomes.SUCCEEDED : CommitOutcomes.FAILED,
          origin, destination,
          exception);
    }

    public CommitFileOutcome(CommitOutcomes outcome,
        String origin,
        String destination,
        IOException exception) {
      if (outcome.equals(CommitOutcomes.FAILED)) {
        Preconditions.checkArgument(exception != null,
            "no exception for failure");
      }
      this.outcome = outcome;
      this.origin = origin;
      this.destination = destination;
      this.exception = exception;
    }

    public CommitOutcomes getOutcome() {
      return outcome;
    }

    public String getDestination() {
      return destination;
    }

    /**
     * Predicate: is this a successful commit operation?
     * @return true if the outcome was SUCCEEDED.
     */
    public boolean isSucceeded() {
      return hasOutcome(CommitOutcomes.SUCCEEDED);
    }

    /**
     * Probe for the outcome being the desired one.
     * @param desired desired outcome.
     * @return true if the outcome is the desired one
     */
    public boolean hasOutcome(CommitOutcomes desired) {
      return outcome == desired;
    }

    public String getOrigin() {
      return origin;
    }

    public IOException getException() {
      return exception;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "CommitFileOutcome{");
      sb.append(outcome);
      sb.append(", destination=").append(destination);
      sb.append(", pendingFile=").append(origin);
      if (exception != null) {
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
