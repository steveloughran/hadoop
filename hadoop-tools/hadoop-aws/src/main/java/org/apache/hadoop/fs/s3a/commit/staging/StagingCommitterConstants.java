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

/**
 * Constants for the committer. All committer-specific options
 * MUST use the "fs.s3a." prefix so as to support per-bucket configuration.
 */
public final class StagingCommitterConstants {

  public static final String STAGING_UPLOADS = "staging-uploads";

  private StagingCommitterConstants() {
  }

  public static final String UPLOAD_SIZE =
      "fs.s3a.staging.committer.upload.size";
  public static final long DEFAULT_UPLOAD_SIZE = 10485760L; // 10 MB
  public static final String UPLOAD_UUID = "fs.s3a.staging.committer.uuid";
  public static final String CONFLICT_MODE =
      "fs.s3a.staging.committer.conflict-mode";
  public static final String CONFLICT_MODE_FAIL = "fail";
  public static final String CONFLICT_MODE_APPEND = "append";
  public static final String CONFLICT_MODE_REPLACE = "replace";
  public static final String DEFAULT_CONFLICT_MODE = CONFLICT_MODE_FAIL;


  public static final String COMMITTER_THREADS =
      "fs.s3a.staging.committer.threads";
  public static final int DEFAULT_COMMITTER_THREADS = 8;

  /**
   * Option for final files to have uniqueness through uuid or attempt info:
   * {@value}.
   * Can be used for conflict resolution, but it also means that the filenames
   * of output may not always be predictable in advance.
   */
  public static final String COMMITTER_UNIQUE_FILENAMES =
      "fs.s3a.staging.committer.unique-filenames";

  /**
   * Default value for {@link #COMMITTER_UNIQUE_FILENAMES}: {@value}.
   */
  public static final boolean DEFAULT_COMMITTER_UNIQUE_FILENAMES = false;

  // Spark configuration keys
  public static final String SPARK_WRITE_UUID =
      "spark.sql.sources.writeJobUUID";
  public static final String SPARK_APP_ID = "spark.app.id";
  public static final String MAPREDUCE_CLUSTER_LOCAL_DIR
      = "mapreduce.cluster.local.dir";

  /**
   * Path for pending data in the cluster FS.
   */
  public static final String COMMITTER_PENDING_DATA_PATH =
      "fs.s3a.staging.committer.pending.path";
  /**
   * Filename of the commit data for a task: {@value}.
   * Bin suffix to make clear this is not any form of text file.
   */
  public static final String COMMIT_FILENAME = "task-commit-data.bin";
  public static final String JAVA_IO_TMPDIR = "java.io.tmpdir";
}
