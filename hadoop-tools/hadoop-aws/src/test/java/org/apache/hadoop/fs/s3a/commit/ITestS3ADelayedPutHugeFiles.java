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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.scale.AbstractSTestS3AHugeFiles;

import java.io.IOException;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;

/**
 * Write a huge file via the commit mechanism, commit it and verify that it is
 * there.
 */
public class ITestS3ADelayedPutHugeFiles extends AbstractSTestS3AHugeFiles {
  private static final Logger LOG = LoggerFactory.getLogger(
      ITestS3ADelayedPutHugeFiles.class);

  private Path finalDirectory;
  private Path pendingDir;
  private Path jobDir;

  /** file used as the destination for the write;
   *  it is never actually created. */
  private Path pendingOutputFile;

  /** The file with the JSON data about the commit. */
  private Path pendingDataFile;

  protected String getBlockOutputBufferName() {
    return Constants.FAST_UPLOAD_BUFFER_DISK;
  }

  /**
   * Create the scale IO conf with the committer enabled.
   * @return the configuration to use for the test FS.
   */
  @Override
  protected Configuration createScaleConfiguration() {
    Configuration conf = super.createScaleConfiguration();
    conf.setBoolean(COMMITTER_ENABLED, true);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();

    // set up the paths for the commit operation
    finalDirectory = new Path(scaleTestDir, "commit");
    pendingDir = new Path(finalDirectory, PENDING_PATH);
    jobDir = new Path(pendingDir, "job_001");
    String filename = "commit.bin";
    hugefile = new Path(finalDirectory, filename);
    pendingOutputFile = new Path(jobDir, filename);
    pendingDataFile = new Path(jobDir, filename + PENDING_SUFFIX);
  }

  @Override
  public void teardown() throws Exception {
    super.teardown();
  }

  /**
   * Returns the path to the pending file, not that of the huge file.
   * @return a file in the job dir
   */
  @Override
  protected Path getPathOfFileToCreate() {
    return pendingOutputFile;
  }

  @Override
  public void test_030_postCreationAssertions() throws Throwable {
    describe("Committing file");
    assertPathDoesNotExist("final file exists", hugefile);
    assertPathDoesNotExist("final file exists", pendingOutputFile);
    S3AFileSystem fs = getFileSystem();

    assertPathExists("No pending file", pendingDataFile);
    ContractTestUtils.NanoTimer timer
        = new ContractTestUtils.NanoTimer();
    FileCommitActions.CommitAllFilesOutcome outcome =
        new FileCommitActions(fs)
            .commitAllPendingFilesInPath(jobDir, false);
    timer.end("time to commit %s", pendingDataFile);
    outcome.maybeRethrow();
    super.test_030_postCreationAssertions();
  }

  @Override
  public void test_040_PositionedReadHugeFile() throws Throwable {
    super.test_040_PositionedReadHugeFile();
  }

  private void skipQuietly(String text) {
    describe("Skipping: %s", text);
  }
  @Override
  public void test_050_readHugeFile() throws Throwable {
    skipQuietly("readHugeFile");
  }

  @Override
  public void test_100_renameHugeFile() throws Throwable {
    skipQuietly("renameHugeFile");
  }

  @Override
  public void test_999_DeleteHugeFiles() throws IOException {
    if (getFileSystem() != null) {
      try {
        getFileSystem().abortOutstandingMultipartUploads(0);
      } catch (IOException e) {
        LOG.info("Exception while purging old uploads", e);
      }
    }
    super.test_999_DeleteHugeFiles();
    delete(pendingDir, true);
  }
}
