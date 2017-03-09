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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

public class TestStagingPartitionedTaskCommit extends StagingTests.TaskCommitterTest<PartitionedStagingCommitter> {
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
  public void testDefault() throws Exception {
    FileSystem mockS3 = getMockS3();

    final PartitionedStagingCommitter committer = newTaskCommitter();

    committer.setupTask(getTAC());
    StagingTests.createTestOutputFiles(relativeFiles,
        committer.getTaskAttemptPath(getTAC()), getTAC().getConfiguration());

    // test failure when one partition already exists
    reset(mockS3);
    when(mockS3
        .exists(new Path(OUTPUT_PATH, relativeFiles.get(0)).getParent()))
        .thenReturn(true);

    StagingTests.assertThrows(
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
    for (InitiateMultipartUploadRequest request : getMockResults().getRequests().values()) {
      Assert.assertEquals(MockS3AFileSystem.BUCKET, request.getBucketName());
      files.add(request.getKey());
    }
    Assert.assertEquals("Should have the right number of uploads",
        relativeFiles.size(), files.size());

    Set<String> expected = Sets.newHashSet();
    for (String relative : relativeFiles) {
      expected.add(OUTPUT_PREFIX +
          "/" + Paths.addUUID(relative, committer.getUUID()));
    }

    Assert.assertEquals("Should have correct paths", expected, files);
  }

  @Test
  public void testFail() throws Exception {
    FileSystem mockS3 = getMockS3();

    getTAC().getConfiguration()
        .set(StagingCommitterConstants.CONFLICT_MODE, "fail");

    final PartitionedStagingCommitter committer = newTaskCommitter();

    committer.setupTask(getTAC());
    StagingTests.createTestOutputFiles(relativeFiles,
        committer.getTaskAttemptPath(getTAC()), getTAC().getConfiguration());

    // test failure when one partition already exists
    reset(mockS3);
    when(mockS3
        .exists(new Path(OUTPUT_PATH, relativeFiles.get(1)).getParent()))
        .thenReturn(true);

    StagingTests.assertThrows(
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
    for (InitiateMultipartUploadRequest request : getMockResults().getRequests().values()) {
      Assert.assertEquals(MockS3AFileSystem.BUCKET, request.getBucketName());
      files.add(request.getKey());
    }
    Assert.assertEquals("Should have the right number of uploads",
        relativeFiles.size(), files.size());

    Set<String> expected = Sets.newHashSet();
    for (String relative : relativeFiles) {
      expected.add(OUTPUT_PREFIX +
          "/" + Paths.addUUID(relative, committer.getUUID()));
    }

    Assert.assertEquals("Should have correct paths", expected, files);
  }

  @Test
  public void testAppend() throws Exception {
    FileSystem mockS3 = getMockS3();

    getTAC().getConfiguration()
        .set(StagingCommitterConstants.CONFLICT_MODE, "append");

    PartitionedStagingCommitter committer = newTaskCommitter();

    committer.setupTask(getTAC());
    StagingTests.createTestOutputFiles(relativeFiles,
        committer.getTaskAttemptPath(getTAC()), getTAC().getConfiguration());

    // test success when one partition already exists
    reset(mockS3);
    when(mockS3
        .exists(new Path(OUTPUT_PATH, relativeFiles.get(2)).getParent()))
        .thenReturn(true);

    committer.commitTask(getTAC());
    Set<String> files = Sets.newHashSet();
    for (InitiateMultipartUploadRequest request : getMockResults().getRequests().values()) {
      Assert.assertEquals(MockS3AFileSystem.BUCKET, request.getBucketName());
      files.add(request.getKey());
    }
    Assert.assertEquals("Should have the right number of uploads",
        relativeFiles.size(), files.size());

    Set<String> expected = Sets.newHashSet();
    for (String relative : relativeFiles) {
      expected.add(OUTPUT_PREFIX +
          "/" + Paths.addUUID(relative, committer.getUUID()));
    }

    Assert.assertEquals("Should have correct paths", expected, files);
  }

  @Test
  public void testReplace() throws Exception {
    // TODO: this committer needs to delete the data that already exists
    // This test should assert that the delete was done
    FileSystem mockS3 = getMockS3();

    getTAC().getConfiguration()
        .set(StagingCommitterConstants.CONFLICT_MODE, "replace");

    PartitionedStagingCommitter committer = newTaskCommitter();

    committer.setupTask(getTAC());
    StagingTests.createTestOutputFiles(relativeFiles,
        committer.getTaskAttemptPath(getTAC()), getTAC().getConfiguration());

    // test success when one partition already exists
    reset(mockS3);
    when(mockS3
        .exists(new Path(OUTPUT_PATH, relativeFiles.get(3)).getParent()))
        .thenReturn(true);

    committer.commitTask(getTAC());
    Set<String> files = Sets.newHashSet();
    for (InitiateMultipartUploadRequest request : getMockResults().getRequests().values()) {
      Assert.assertEquals(MockS3AFileSystem.BUCKET, request.getBucketName());
      files.add(request.getKey());
    }
    Assert.assertEquals("Should have the right number of uploads",
        relativeFiles.size(), files.size());

    Set<String> expected = Sets.newHashSet();
    for (String relative : relativeFiles) {
      expected.add(OUTPUT_PREFIX +
          "/" + Paths.addUUID(relative, committer.getUUID()));
    }

    Assert.assertEquals("Should have correct paths", expected, files);
  }
}
