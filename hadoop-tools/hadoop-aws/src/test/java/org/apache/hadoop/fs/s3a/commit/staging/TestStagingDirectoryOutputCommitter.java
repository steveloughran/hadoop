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

package org.apache.hadoop.fs.s3a.commit.staging;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathExistsException;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.Callable;

import static org.apache.hadoop.fs.s3a.commit.staging.StagingCommitterConstants.*;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingTestBase.*;
import static org.mockito.Mockito.*;

public class TestStagingDirectoryOutputCommitter
    extends StagingTestBase.JobCommitterTest<DirectoryStagingCommitter> {
  @Override
  DirectoryStagingCommitter newJobCommitter() throws Exception {
    return new DirectoryStagingCommitter(OUTPUT_PATH, getJob());
  }

  @Test
  public void testBadConflictMode() throws Throwable {
    getJob().getConfiguration().set(CONFLICT_MODE, "merge");
    assertThrows("commiter conflict", IllegalArgumentException.class,
        "MERGE", this::newJobCommitter);
  }

  @Test
  public void testDefaultConflictResolution() throws Exception {
    getJob().getConfiguration().unset(CONFLICT_MODE);
    verifyFailureConflictOutcome();
  }
  @Test
  public void testFailConflictResolution() throws Exception {
    getJob().getConfiguration().set(CONFLICT_MODE, CONFLICT_MODE_FAIL);
    verifyFailureConflictOutcome();
  }

  protected void verifyFailureConflictOutcome() throws Exception {
    FileSystem mockS3 = getMockS3();
    pathExists(mockS3, OUTPUT_PATH);
    final DirectoryStagingCommitter committer = newJobCommitter();

    assertThrows("Should throw an exception because the path exists",
        PathExistsException.class, new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            committer.setupJob(getJob());
            return null;
          }
        });

    assertThrows("Should throw an exception because the path exists",
        PathExistsException.class, new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            committer.commitJob(getJob());
            return null;
          }
        });

    reset(mockS3);
    pathDoesNotExist(mockS3, OUTPUT_PATH);

    committer.setupJob(getJob());
    verifyExistenceChecked(mockS3, OUTPUT_PATH);
    verifyNoMoreInteractions(mockS3);

    reset(mockS3);
    pathDoesNotExist(mockS3, OUTPUT_PATH);
    committer.commitJob(getJob());
    verifyExistenceChecked(mockS3, OUTPUT_PATH);
    verifyCompletion(mockS3);
  }

  @Test
  public void testAppendConflictResolution() throws Exception {
    FileSystem mockS3 = getMockS3();

    pathExists(mockS3, OUTPUT_PATH);

    getJob().getConfiguration().set(CONFLICT_MODE, CONFLICT_MODE_APPEND);

    final DirectoryStagingCommitter committer = newJobCommitter();

    committer.setupJob(getJob());
    verifyExistenceChecked(mockS3, OUTPUT_PATH);

    Mockito.reset(mockS3);
    pathExists(mockS3, OUTPUT_PATH);

    committer.commitJob(getJob());
//    verifyExistenceChecked(mockS3, OUTPUT_PATH);
    verifyCompletion(mockS3);
  }

  @Test
  public void testReplaceConflictResolution() throws Exception {
    FileSystem mockS3 = getMockS3();

    pathExists(mockS3, OUTPUT_PATH);

    getJob().getConfiguration().set(CONFLICT_MODE, CONFLICT_MODE_REPLACE);

    final DirectoryStagingCommitter committer = newJobCommitter();

    committer.setupJob(getJob());
    verifyExistenceChecked(mockS3, OUTPUT_PATH);
    verifyNoMoreInteractions(mockS3);

    Mockito.reset(mockS3);
    pathExists(mockS3, OUTPUT_PATH);
    canDelete(mockS3, OUTPUT_PATH);

    committer.commitJob(getJob());
//    verifyExistenceChecked(mockS3, OUTPUT_PATH);
    verifyDeleted(mockS3, OUTPUT_PATH);
    verifyCompletion(mockS3);
  }

}
