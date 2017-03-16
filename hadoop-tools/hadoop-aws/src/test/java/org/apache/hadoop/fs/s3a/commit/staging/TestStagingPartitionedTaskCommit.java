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

import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.mockito.Mockito.*;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingTestBase.*;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingCommitterConstants.*;

/** Test suite.*/
public class TestStagingPartitionedTaskCommit
    extends StagingTestBase.TaskCommitterTest<PartitionedStagingCommitter> {
  @Override
  PartitionedStagingCommitter newJobCommitter() throws IOException {
    return new PartitionedCommitterForTesting(OUTPUT_PATH,
        getJob(),
        getMockClient());
  }

  @Override
  PartitionedStagingCommitter newTaskCommitter() throws Exception {
    return new PartitionedCommitterForTesting(
        OUTPUT_PATH,
        getTAC(),
        getMockClient());
  }

  // The set of files used by this test
  private static List<String> relativeFiles = Lists.newArrayList();

  @BeforeClass
  public static void createRelativeFileList() {
    for (String dateint : Arrays.asList("20161115", "20161116")) {
      for (String hour : Arrays.asList("14", "15")) {
        String relative = "dateint=" + dateint + "/hour=" + hour +
            "/" + UUID.randomUUID().toString() + ".parquet";
        relativeFiles.add(relative);
      }
    }
  }


  @Test
  public void testBadConflictMode() throws Throwable {
    getJob().getConfiguration().set(CONFLICT_MODE, "merge");
    assertThrows("commiter conflict", IllegalArgumentException.class,
        "MERGE", this::newJobCommitter);
  }

  @Test
  public void testDefault() throws Exception {
    FileSystem mockS3 = getMockS3();

    JobContext job = getJob();
    job.getConfiguration().unset(CONFLICT_MODE);
    final PartitionedStagingCommitter committer = newTaskCommitter();

    committer.setupTask(getTAC());
    assertConflictResolution(committer, job, ConflictResolution.FAIL);
    createTestOutputFiles(relativeFiles,
        committer.getTaskAttemptPath(getTAC()), getTAC().getConfiguration());

    // test failure when one partition already exists
    reset(mockS3);
    pathExists(mockS3, new Path(OUTPUT_PATH, relativeFiles.get(0)).getParent());

    assertThrows(
        "Expected a PathExistsException as a partition already exists",
        PathExistsException.class, new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            committer.commitTask(getTAC());
            return null;
          }
        });

    // test success
    reset(mockS3);

    committer.commitTask(getTAC());
    Set<String> files = Sets.newHashSet();
    for (InitiateMultipartUploadRequest request :
        getMockResults().getRequests().values()) {
      assertEquals(BUCKET, request.getBucketName());
      files.add(request.getKey());
    }
    assertEquals("Should have the right number of uploads",
        relativeFiles.size(), files.size());

    Set<String> expected = Sets.newHashSet();
    for (String relative : relativeFiles) {
      expected.add(OUTPUT_PREFIX +
          "/" + Paths.addUUID(relative, committer.getUUID()));
    }

    assertEquals("Should have correct paths", expected, files);
  }

  @Test
  public void testFail() throws Exception {
    FileSystem mockS3 = getMockS3();

    getTAC().getConfiguration()
        .set(CONFLICT_MODE, CONFLICT_MODE_FAIL);

    final PartitionedStagingCommitter committer = newTaskCommitter();

    committer.setupTask(getTAC());
    createTestOutputFiles(relativeFiles,
        committer.getTaskAttemptPath(getTAC()), getTAC().getConfiguration());

    // test failure when one partition already exists
    reset(mockS3);
    pathExists(mockS3, new Path(OUTPUT_PATH, relativeFiles.get(1)).getParent());

    assertThrows(
        "Should complain because a partition already exists",
        PathExistsException.class, new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            committer.commitTask(getTAC());
            return null;
          }
        });

    // test success
    reset(mockS3);

    committer.commitTask(getTAC());
    Set<String> files = Sets.newHashSet();
    for (InitiateMultipartUploadRequest request :
        getMockResults().getRequests().values()) {
      assertEquals(BUCKET, request.getBucketName());
      files.add(request.getKey());
    }
    assertEquals("Should have the right number of uploads",
        relativeFiles.size(), files.size());

    Set<String> expected = Sets.newHashSet();
    for (String relative : relativeFiles) {
      expected.add(OUTPUT_PREFIX +
          "/" + Paths.addUUID(relative, committer.getUUID()));
    }

    assertEquals("Should have correct paths", expected, files);
  }

  @Test
  public void testAppend() throws Exception {
    FileSystem mockS3 = getMockS3();

    getTAC().getConfiguration()
        .set(CONFLICT_MODE, CONFLICT_MODE_APPEND);

    PartitionedStagingCommitter committer = newTaskCommitter();

    committer.setupTask(getTAC());
    createTestOutputFiles(relativeFiles,
        committer.getTaskAttemptPath(getTAC()), getTAC().getConfiguration());

    // test success when one partition already exists
    reset(mockS3);
    pathExists(mockS3, new Path(OUTPUT_PATH, relativeFiles.get(2)).getParent());

    committer.commitTask(getTAC());
    Set<String> files = Sets.newHashSet();
    for (InitiateMultipartUploadRequest request :
        getMockResults().getRequests().values()) {
      assertEquals(BUCKET, request.getBucketName());
      files.add(request.getKey());
    }
    assertEquals("Should have the right number of uploads",
        relativeFiles.size(), files.size());

    Set<String> expected = Sets.newHashSet();
    for (String relative : relativeFiles) {
      expected.add(OUTPUT_PREFIX +
          "/" + Paths.addUUID(relative, committer.getUUID()));
    }

    assertEquals("Should have correct paths", expected, files);
  }

  @Test
  public void testReplace() throws Exception {
    // TODO: this committer needs to delete the data that already exists
    // This test should assert that the delete was done
    FileSystem mockS3 = getMockS3();

    getTAC().getConfiguration()
        .set(CONFLICT_MODE, CONFLICT_MODE_REPLACE);

    PartitionedStagingCommitter committer = newTaskCommitter();

    committer.setupTask(getTAC());
    createTestOutputFiles(relativeFiles,
        committer.getTaskAttemptPath(getTAC()), getTAC().getConfiguration());

    // test success when one partition already exists
    reset(mockS3);
    pathExists(mockS3, new Path(OUTPUT_PATH, relativeFiles.get(3)).getParent());

    committer.commitTask(getTAC());
    Set<String> files = Sets.newHashSet();
    for (InitiateMultipartUploadRequest request :
        getMockResults().getRequests().values()) {
      assertEquals(BUCKET, request.getBucketName());
      files.add(request.getKey());
    }
    assertEquals("Should have the right number of uploads",
        relativeFiles.size(), files.size());

    Set<String> expected = Sets.newHashSet();
    for (String relative : relativeFiles) {
      expected.add(OUTPUT_PREFIX +
          "/" + Paths.addUUID(relative, committer.getUUID()));
    }

    assertEquals("Should have correct paths", expected, files);
  }
}
