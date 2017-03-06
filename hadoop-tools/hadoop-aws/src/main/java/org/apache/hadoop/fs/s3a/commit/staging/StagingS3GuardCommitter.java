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
/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.commit.staging;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.AbstractS3GuardCommitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingCommitterConstants.*;

/**
 * Committer based on the architecture of the
 * <a href="https://github.com/rdblue/s3committer">Netflix multipart committers.</a>
 * <ol>
 *   <li>
 *   The working directory of each task is actually under a temporary
 *   path in the local filesystem; jobs write directly into it.
 *   </li>
 *   <li>
 *     Task Commit: list all files under the task working dir, upload
 *     each of them but do not commit the final operation.
 *     Persist the information for each pending commit into a directory
 *     (ISSUE: which FS?) for enumeration by the job committer.
 *   </li>
 *   <li>Task Abort: recursive delete of task working dir.</li>
 *   <li>Job Commit: list all pending PUTs to commit; commit them.</li>
 *   <li>
 *     Job Abort: list all pending PUTs to commit; abort them.
 *     Delete all task attempt directories.
 *   </li>
 * </ol>
 * <p>
 * Configuration options:
 * <pre>
 *   : temporary local FS directory
 *   : intermediate directory on a cluster-wide FS (can be HDFS or a consistent
 *   s3 endpoint).
 *
 * </pre>
 */

public class StagingS3GuardCommitter extends AbstractS3GuardCommitter {


  private static final Logger LOG = LoggerFactory.getLogger(
      StagingS3GuardCommitter.class);
  private final Path constructorOutputPath;
  private final long uploadPartSize;
  private final String uuid;
  private final Path workPath;
  private ConflictResolution mode = null;
  private ExecutorService threadPool = null;
  private Path finalOutputPath = null;
  private String bucket = null;
  private String s3KeyPrefix = null;
  private Path bucketRoot = null;
  
  public StagingS3GuardCommitter(Path outputPath,
      JobContext context) throws IOException {
    super(outputPath, context);
    constructorOutputPath = getOutputPath();
    Configuration conf = getConf();
    uploadPartSize = conf.getLong(
        UPLOAD_SIZE, DEFAULT_UPLOAD_SIZE);
    // Spark will use a fake app id based on the current minute and job id 0.
    // To avoid collisions, use the YARN application ID for Spark.
    uuid = getUploadUUID(conf, context.getJobID().toString());
    workPath = buildWorkPath(context);
  }


  public StagingS3GuardCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    constructorOutputPath = getOutputPath();
    Configuration conf = getConf();
    uploadPartSize = conf.getLong(
        UPLOAD_SIZE, DEFAULT_UPLOAD_SIZE);
    // Spark will use a fake app id based on the current minute and job id 0.
    // To avoid collisions, use the YARN application ID for Spark.
    uuid = getUploadUUID(conf, context.getJobID().toString());
    workPath = buildWorkPath(context);
  }


  private static String getUploadUUID(Configuration conf, String jobId) {
    return conf.get(UPLOAD_UUID, conf.get(
        SPARK_WRITE_UUID,
        conf.get(SPARK_APP_ID, jobId)));
  }

  private Path buildWorkPath(JobContext context) throws IOException {
    if (context instanceof TaskAttemptContext) {
      return taskAttemptPath((TaskAttemptContext) context, uuid);
    } else {
      return null;
    }
  }

  private static Path taskAttemptPath(TaskAttemptContext context, String uuid)
  throws IOException {
    return getTaskAttemptPath(context, Paths.getLocalTaskAttemptTempDir(
        context.getConfiguration(), uuid,
        getTaskId(context), getAttemptId(context)));
  }

  private static int getTaskId(TaskAttemptContext context) {
    return context.getTaskAttemptID().getTaskID().getId();
  }

  private static int getAttemptId(TaskAttemptContext context) {
    return context.getTaskAttemptID().getId();
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {

  }

  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException {

  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext)
      throws IOException {
    return false;
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {

  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException {

  }

  @Override
  protected boolean isDelayedCommitRequired() {
    return false;
  }

  @Override
  public Path getTaskAttemptPath(TaskAttemptContext context) {
    return super.getTaskAttemptPath(context);
  }
}
