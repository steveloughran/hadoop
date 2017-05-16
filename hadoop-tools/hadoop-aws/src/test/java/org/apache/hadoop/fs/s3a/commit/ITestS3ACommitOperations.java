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
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.fs.s3a.commit.CommitUtils.*;

/**
 * Test the low-level binding of the S3A FS to the magic commit mechanism,
 * and handling of the commit operations.
 */
public class ITestS3ACommitOperations extends AbstractCommitITest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ACommitOperations.class);
  private static final byte[] DATASET = dataset(1000, 'a', 32);

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setBoolean("fs.s3a.impl.disable.cache", true);
    conf.setBoolean(MAGIC_COMMITTER_ENABLED, true);
    conf.set(PathOutputCommitterFactory.OUTPUTCOMMITTER_FACTORY_CLASS,
        MagicS3GuardCommitterFactory.CLASSNAME);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
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

    Path pendingSuffixedPath = new Path(pendingPath,
        "part-0000" + PENDING_SUFFIX);
    assertFalse("still a delayed complete path " + pendingSuffixedPath,
        fs.isDelayedCompletePath(pendingSuffixedPath));
    Path pendingSet = new Path(pendingPath,
        "part-0000" + PENDINGSET_SUFFIX);
    assertFalse("still a delayed complete path " + pendingSet,
        fs.isDelayedCompletePath(pendingSet));
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

    CommitActions actions = newActions();
    // abort,; rethrow on failure
    LOG.info("First abort call");
    actions.abortAllSinglePendingCommits(pendingDataPath.getParent(), true)
        .maybeRethrow();
    assertPathDoesNotExist("pending file not deleted", pendingDataPath);
    assertPathDoesNotExist("dest file was created", destFile);
  }

  public CommitActions newActions() {
    return new CommitActions(getFileSystem());
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
        MAGIC_DIR_NAME + '/' + name);
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

  @Test
  public void testAbortNonexistentDir() throws Throwable {
    describe("Attempt to abort a directory that does not exist");
    Path destFile = methodPath("testAbortNonexistentPath");
    newActions()
        .abortAllSinglePendingCommits(destFile, true)
        .maybeRethrow();
  }

  @Test
  public void testCommitterFactory() throws Throwable {
    Configuration conf = new Configuration();
    conf.set(PathOutputCommitterFactory.OUTPUTCOMMITTER_FACTORY_CLASS,
        MagicS3GuardCommitterFactory.CLASSNAME);
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
    describe("Test creating file with a __base marker and verify that it ends" +
        " up in where expected");
    Path destDir = methodPath("testBaseRelativePath");
    Path pendingBaseDir = new Path(destDir,
        MAGIC_DIR_NAME + "/child/"
            + BASE_PATH);
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
   * Commit to a write to {@code pendingFilePath} which is expected to
   * be saved to {@code destFile}.
   * @param pendingFilePath path to write to
   * @param destFile destination to verify
   */
  private void commit(String filename, Path pendingFilePath, Path destFile)
      throws IOException {
    assertPathDoesNotExist("pending file was created", pendingFilePath);
    assertPathDoesNotExist("dest file was created", destFile);
    Path pendingDataPath = validatePendingCommitData(filename,
        pendingFilePath);
    SinglePendingCommit commit = SinglePendingCommit.load(getFileSystem(),
        pendingDataPath);
    CommitActions actions = newActions();
    actions.commitOrFail(commit);
    verifyCommitExists(commit);
  }


  /**
   * Verify that the path at the end of a commit exists.
   * This does not validate the size.
   * @param commit commit to verify
   * @throws FileNotFoundException dest doesn't exist
   * @throws ValidationFailure commit arg is invalid
   * @throws IOException invalid commit, IO failure
   */
  public void verifyCommitExists(SinglePendingCommit commit)
      throws FileNotFoundException, ValidationFailure, IOException {
    commit.validate();
    // this will force an existence check
    Path path = getFileSystem().keyToQualifiedPath(commit.getDestinationKey());
    FileStatus status = getFileSystem().getFileStatus(path);
    LOG.debug("Destination entry: {}", status);
    if (!status.isFile()) {
      throw new PathCommitException(path, "Not a file: " + status);
    }
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
        filename + PENDING_SUFFIX);
    FileStatus fileStatus = verifyPathExists(fs, "no pending data",
        pendingDataPath);
    assertTrue("No data in " + fileStatus, fileStatus.getLen() > 0);
    String data = read(fs, pendingDataPath);
    LOG.info("Contents of {}: \n{}", pendingDataPath, data);
    // really read it in and parse
    SinglePendingCommit persisted = SinglePendingCommit.serializer()
        .load(fs, pendingDataPath);
    persisted.validate();
    assertTrue("created timestamp wrong in " + persisted,
        persisted.getCreated() > 0);
    assertTrue("saved timestamp wrong in " + persisted,
        persisted.getSaved() > 0);
    List<String> etags = persisted.getEtags();
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
