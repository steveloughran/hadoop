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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import com.amazonaws.services.s3.model.PartETag;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.magic.MagicCommitterConstants;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitUtils.*;

/**
 * Test the various commit operations
 * {@link S3AFileSystem#getFileStatus(Path)}.
 */
public class ITestS3ACommitOperations extends AbstractCommitITest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ACommitOperations.class);
  private static final byte[] DATASET = dataset(1000, 'a', 32);

  @Test
  public void testVerifyIsDelayedCompleteFS() throws Throwable {
    verifyIsMagicCommitFS(getFileSystem());
  }

  @Test
  public void testDelayedCompleteIntegrationNotPending() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    DelayedCommitFSIntegration integration
        = new DelayedCommitFSIntegration(fs, true);
    String filename = "notdelayed.txt";
    Path destFile = methodPath(filename);
    String origKey = fs.pathToKey(destFile);
    DefaultPutTracker tracker = integration.getTracker(destFile, origKey);
    assertFalse("wrong type: " + tracker + " for " + destFile,
        tracker instanceof DelayedCommitTracker);
  }

  @Test
  public void testDelayedCompleteIntegration() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    DelayedCommitFSIntegration integration
        = new DelayedCommitFSIntegration(fs, true);
    String filename = "delayed.txt";
    Path destFile = methodPath(filename);
    String origKey = fs.pathToKey(destFile);
    Path pendingPath = makePending(destFile);
    verifyIsDelayedCommitPath(fs, pendingPath);
    String pendingPathKey = fs.pathToKey(pendingPath);
    assertTrue("wrong path of " + pendingPathKey,
        pendingPathKey.endsWith(filename));
    final List<String> elements = splitPathToElements(pendingPath);
    assertEquals("splitPathToElements()", filename, lastElement(elements));
    List<String> finalDestination = finalDestination(elements);
    assertEquals("finalDestination()",
        filename,
        lastElement(finalDestination));
    final String destKey = elementsToKey(finalDestination);
    assertEquals("destination key", origKey, destKey);

    DefaultPutTracker tracker = integration.getTracker(pendingPath,
        pendingPathKey);
    assertTrue("wrong type: " + tracker + " for " + pendingPathKey,
        tracker instanceof DelayedCommitTracker);
    assertEquals("tracker destination key", origKey, tracker.getDestKey());
  }

  @Test
  public void testCreateAbortEmptyFile() throws Throwable {
    describe("create then abort an empty file");
    S3AFileSystem fs = getFileSystem();
    String filename = "empty-abort.txt";
    Path destFile = methodPath(filename);
    Path pendingFilePath = makePending(destFile);
    touch(fs, pendingFilePath);
    assertPathDoesNotExist("pending file was created", pendingFilePath);
    assertPathDoesNotExist("dest file was created", destFile);
    Path pendingDataPath = validatePendingCommitData(filename,
        pendingFilePath);

    FileCommitActions actions = newActions();
    // abort,; rethrow on failure
    LOG.info("First abort call");
    actions.abortPendingFile(pendingDataPath).maybeRethrow();
    assertPathDoesNotExist("pending file not deleted", pendingDataPath);
    assertPathDoesNotExist("dest file was created", destFile);

    // and again. here uprating a missing file to a failure
    LOG.info("Second abort call");
    FileCommitActions.CommitFileOutcome outcome = actions.abortPendingFile(
        pendingDataPath);
    assertTrue("Expected 2nd abort to fail to ABORT_FAILED " + outcome,
        outcome.hasOutcome(FileCommitActions.CommitOutcomes.ABORT_FAILED));
    if (!(outcome.getException() instanceof FileNotFoundException)) {
      outcome.maybeRethrow();
    }
  }

  public FileCommitActions newActions() {
    return new FileCommitActions(getFileSystem());
  }

  /**
   * Create a new path which has the same filename as the dest file, but
   * is in a pending directory under the destination dir.
   * @param destFile final destination file
   * @return pending path
   */
  protected static Path makePending(Path destFile) {
    return makePendingChild(destFile, destFile.getName());
  }

  private static Path makePendingChild(Path destFile, String name) {
    return new Path(destFile.getParent(),
        MagicCommitterConstants.MAGIC_DIR_NAME + '/' + name);
  }

  @Test
  public void testCommitEmptyFile() throws Throwable {
    describe("create then commit an empty file");
    createCommitAndVerify("empty-commit.txt", new byte[0]);
  }

  @Test
  public void testCommitSmallFile() throws Throwable {
    describe("create then commit an empty file");
    createCommitAndVerify("small-commit.txt", DATASET);
  }

  @Test(expected = FileNotFoundException.class)
  public void testCommitNonexistentDir() throws Throwable {
    describe("Attempt to commit a pending directory that does not exist");
    Path destFile = methodPath("testCommitNonexistentDir");
    newActions()
        .commitSinglePendingCommits(destFile, true);
  }

  @Test(expected = PathCommitException.class)
  public void testCommitPendingFilesinSimpleFile() throws Throwable {
    describe("Attempt to commit a pending directory that is actually a file");
    Path destFile = methodPath("testCommitPendingFilesinSimpleFile");
    touch(getFileSystem(), destFile);
    newActions()
        .commitSinglePendingCommits(destFile, true);
  }

  @Test
  public void testAbortNonexistentDir() throws Throwable {
    describe("Attempt to abort a directory that does not exist");
    Path destFile = methodPath("testAbortNonexistentPath");
    FileCommitActions.CommitAllFilesOutcome outcome = newActions()
        .abortAllPendingFilesInPath(destFile, true);
    outcome.maybeRethrow();
    assertFalse("outcome includes successes",
        outcome.hasOutcome(FileCommitActions.CommitOutcomes.SUCCEEDED));
  }

  @Test
  public void testAbortNonexistentFile() throws Throwable {
    describe("Attempt to abort a directory that does not exist");
    Path destFile = methodPath("testAbortNonexistentFile");
    FileCommitActions.CommitFileOutcome outcome =
        newActions().abortPendingFile(destFile);
    assertTrue("succeeded: " + outcome, outcome.isSucceeded());
  }

  @Test
  public void testCommitterFactory() throws Throwable {
    Configuration conf = new Configuration();
    conf.set(PathOutputCommitterFactory.OUTPUTCOMMITTER_FACTORY_CLASS,
        MagicS3GuardCommitterFactory.NAME);
    PathOutputCommitterFactory factory
        = PathOutputCommitterFactory.getOutputCommitterFactory(conf);
    PathOutputCommitter committer = factory.createOutputCommitter(
        path("testFactory"),
        new TaskAttemptContextImpl(getConfiguration(),
            new TaskAttemptID(new TaskID(), 1)));
    MagicS3GuardCommitter s3a = (MagicS3GuardCommitter) committer;
  }

  @Test
  public void testBaseRelativePath() throws Throwable {
    Path destDir = methodPath("testBaseRelativePath");
    Path pendingBaseDir = new Path(destDir,
        MagicCommitterConstants.MAGIC_DIR_NAME + "/child/" + MagicCommitterConstants.BASE_PATH);
    String child = "subdir/child.txt";
    Path pendingChildPath = new Path(pendingBaseDir, child);
    Path expectedDestPath = new Path(destDir, child);
    createFile(getFileSystem(), pendingChildPath, true, DATASET);
    commit("child.txt", pendingChildPath, expectedDestPath);
  }

  protected void createCommitAndVerify(String filename, byte[] data)
      throws Exception {
    S3AFileSystem fs = getFileSystem();
    Path destFile = methodPath(filename);
    Path pendingFilePath = makePending(destFile);
    createFile(fs, pendingFilePath, true, data);
    commit(filename, destFile);
    verifyFileContents(fs, destFile, data);
  }

  /**
   * Commit the file, with before and after checks on the dest and pending
   * values.
   * @param filename filename of file
   * @param destFile destination path of file
   * @throws Exception any failure of the operation
   */
  private void commit(String filename, Path destFile) throws Exception {
    commit(filename, makePending(destFile), destFile);
  }

  /**
   * Commit to a write to {@code pendingFilePath} which is expected to saved
   * to {@code destFile}.
   * @param pendingFilePath path to write to
   * @param destFile destination to verify
   */
  private void commit(String filename, Path pendingFilePath, Path destFile)
      throws IOException {
    assertPathDoesNotExist("pending file was created", pendingFilePath);
    assertPathDoesNotExist("dest file was created", destFile);
    Path pendingDataPath = validatePendingCommitData(filename,
        pendingFilePath);
    FileCommitActions.CommitFileOutcome outcome =
        newActions().commitPendingFile(pendingDataPath);
    outcome.maybeRethrow();
    assertPathDoesNotExist("pending file not deleted after " + outcome,
        pendingDataPath);
    assertPathExists("dest file " + destFile + " not committed " + outcome,
        destFile);
  }

  /**
   * Validate that a pending commit data file exists, load it and validate
   * its contents.
   * @param filename short file name
   * @param pendingFilePath path that the file thinks that it was written to
   * @return the path to the pending data
   * @throws IOException IO problems
   */
  protected Path validatePendingCommitData(String filename,
      Path pendingFilePath) throws IOException {
    S3AFileSystem fs = getFileSystem();
    Path pendingDataPath = new Path(pendingFilePath.getParent(),
        filename + MagicCommitterConstants.PENDING_SUFFIX);
    FileStatus fileStatus = verifyPathExists(fs, "no pending data",
        pendingDataPath);
    assertTrue("No data in " + fileStatus, fileStatus.getLen() > 0);
    String data = read(fs, pendingDataPath);
    LOG.info("Contents of {}: \n{}", pendingDataPath, data);
    // really read it in and parse
    SinglePendingCommit persisted = SinglePendingCommit.getSerializer()
        .load(fs, pendingDataPath);
    persisted.validate();
    assertTrue("created timestamp wrong in " + persisted,
        persisted.created > 0);
    assertTrue("saved timestamp wrong in " + persisted,
        persisted.saved > 0);
    List<String> etags = persisted.etags;
    assertEquals("etag list " + persisted, 1, etags.size());
    List<PartETag> partList = CommitUtils.toPartEtags(etags);
    assertEquals("part list " + persisted, 1, partList.size());
    return pendingDataPath;
  }

  /**
   * Get a method-relative path.
   * @param filename filename
   * @return new path
   * @throws IOException failure to create/parse the path.
   */
  public Path methodPath(String filename) throws IOException {
    return new Path(path(getMethodName()), filename);
  }

}
