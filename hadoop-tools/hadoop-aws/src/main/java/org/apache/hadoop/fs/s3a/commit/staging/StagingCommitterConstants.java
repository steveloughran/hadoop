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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.MRConfig;

import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;

/**
 * Internal staging committer constants.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class StagingCommitterConstants {

  public static final String STAGING_UPLOADS = "staging-uploads";

  private StagingCommitterConstants() {
  }

  // Spark configuration keys

  /**
   * The UUID for jobs: {@value}.
   */
  public static final String SPARK_WRITE_UUID =
      "spark.sql.sources.writeJobUUID";

  /**
   * The App ID for jobs.
   */

  public static final String SPARK_APP_ID = "spark.app.id";

  /**
   * This constant is lifted from the {@code MRConfig} class, which
   * is tagged private. Putting here makes is public, albeit unstable.
   */
  public static final String MAPREDUCE_CLUSTER_LOCAL_DIR =
      MRConfig.LOCAL_DIR;

  /**
   * Filename of the commit data for a task: {@value}.
   * Bin suffix to make clear this is not any form of text file.
   */
  public static final String COMMIT_FILENAME = "task-commit-data.bin";
  public static final String JAVA_IO_TMPDIR = "java.io.tmpdir";
}
